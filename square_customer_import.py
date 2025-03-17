import os
import sys
import csv
import json
import uuid
import logging
import pandas as pd
from datetime import datetime
from square.client import Client
from dotenv import load_dotenv
from tqdm import tqdm

# 加载环境变量
load_dotenv()

class SquareCustomerImport:
    def __init__(self, access_token):
        self.client = Client(
            access_token=access_token,
            environment=os.getenv('SQUARE_ENVIRONMENT', 'sandbox')
        )
        self.setup_logging()
    
    def setup_logging(self):
        """设置日志记录"""
        # 确保logs文件夹存在
        os.makedirs('logs', exist_ok=True)
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(f'logs/import_log_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def format_phone_number(self, phone):
        """格式化电话号码，仅在国际区号前添加加号"""
        if not phone:
            return ""
        # 将电话号码转换为字符串并去除首尾空白
        phone = str(phone).strip()
        # 如果已经包含加号，则直接返回
        if phone.startswith('+'):
            return phone
        # 如果不包含加号，则在开头添加加号
        return f"+{phone}"

    def process_name(self, name):
        """处理姓名格式，按照斜线'/'拆分姓和名"""
        if not name:
            return {
                'given_name': '',
                'family_name': ''
            }
        # 如果包含斜线，则按斜线拆分为姓和名
        if '/' in name:
            family_name, given_name = name.split('/', 1)
            return {
                'given_name': given_name.strip(),
                'family_name': family_name.strip()
            }
        # 如果没有斜线，则将整个名字作为名
        return {
            'given_name': name.strip(),
            'family_name': ''
        }

    def read_file(self, file_path):
        """读取客户数据文件（支持CSV和Excel格式）"""
        customers = []
        try:
            file_ext = os.path.splitext(file_path)[1].lower()
            if file_ext == '.csv':
                with open(file_path, 'r', encoding='utf-8') as file:
                    reader = csv.DictReader(file)
                    for row in reader:
                        customers.append(row)
            elif file_ext in ['.xlsx', '.xls']:
                df = pd.read_excel(file_path)
                for _, row in df.iterrows():
                    name_parts = self.process_name(str(row.get('Customer name', '')))
                    customer = {
                        'given_name': name_parts['given_name'],
                        'family_name': name_parts['family_name'],
                        'email_address': str(row.get('Customer email', '')),
                        'phone_number': self.format_phone_number(row.get('Customer phone number', ''))
                    }
                    customers.append(customer)
            else:
                raise ValueError(f'不支持的文件格式: {file_ext}')
            return customers
        except Exception as e:
            self.logger.error(f'读取文件失败: {str(e)}')
            return None
    
    def validate_customer_data(self, customer):
        """验证客户数据的完整性和格式"""
        # 验证必填字段之一是否存在
        has_required_field = any([
            customer.get('given_name'),
            customer.get('family_name'),
            customer.get('company_name'),
            customer.get('email_address'),
            customer.get('phone_number')
        ])
        if not has_required_field:
            return False
        
        # 验证邮箱格式
        email = customer.get('email_address', '')
        if email and ('@' not in email or '.' not in email):
            return False
        
        # 验证电话号码格式（可选）
        phone = customer.get('phone_number', '')
        if phone and not phone.startswith('+'):
            return False
            
        return True
    
    def check_duplicate_customer(self, email=None, phone=None):
        """检查是否存在重复的客户"""
        if not email and not phone:
            return False

        query = {
            'query': {
                'filter': {
                    'or': []
                }
            }
        }

        if email:
            query['query']['filter']['or'].append({
                'email_address': {'exact': email}
            })

        if phone:
            query['query']['filter']['or'].append({
                'phone_number': {'exact': phone}
            })

        try:
            result = self.client.customers.search_customers(body=query)
            if result.is_success() and result.body.get('customers'):
                return True
            return False
        except Exception as e:
            self.logger.warning(f'检查重复客户时发生错误: {str(e)}')
            return False

    def create_customers_batch(self, customers_data, group_id):
        """批量创建客户"""
        try:
            batch_size = 300
            total_success = 0
            total_failed = 0
            all_responses = {}
            
            # 创建总进度条
            total_pbar = tqdm(total=len(customers_data), desc='总体进度', unit='客户')

            for i in range(0, len(customers_data), batch_size):
                batch_customers = customers_data[i:i + batch_size]
                customers_dict = {}
                
                # 创建批次进度条
                batch_pbar = tqdm(total=len(batch_customers), desc=f'第 {i//batch_size + 1} 批', 
                                 unit='客户', leave=False)

                for customer in batch_customers:
                    if self.check_duplicate_customer(
                        email=customer.get('email_address'),
                        phone=customer.get('phone_number')
                    ):
                        self.logger.warning(
                            f'发现重复客户: {customer.get("email_address")} / {customer.get("phone_number")}'
                        )
                        total_failed += 1
                        total_pbar.update(1)
                        batch_pbar.update(1)
                        continue

                    customer_id = str(uuid.uuid4())
                    customers_dict[customer_id] = {
                        'given_name': customer.get('given_name'),
                        'family_name': customer.get('family_name', ''),
                        'company_name': customer.get('company_name', ''),
                        'email_address': customer.get('email_address'),
                        'phone_number': customer.get('phone_number', ''),
                        'note': customer.get('note', '')
                    }
                    batch_pbar.update(1)
                
                if not customers_dict:
                    batch_pbar.close()
                    continue

                result = self.client.customers.bulk_create_customers(
                    body={
                        'customers': customers_dict
                    }
                )
                
                if result.is_success():
                    responses = result.body.get('responses', {})
                    all_responses.update(responses)
                    successful_customer_ids = []
                    for key, response in responses.items():
                        if 'errors' in response:
                            self.logger.warning(f'客户 {key} 创建失败: {response["errors"]}')
                            total_failed += 1
                        else:
                            # 先不增加成功计数，等待添加到群组后再确认
                            successful_customer_ids.append(response['customer']['id'])
                        total_pbar.update(1)
                    
                    # 将成功创建的客户添加到组中
                    if successful_customer_ids:
                        self.logger.info(f'尝试将 {len(successful_customer_ids)} 个客户添加到群组...')
                        if self.add_customers_to_group(group_id, successful_customer_ids):
                            self.logger.info(f'成功将 {len(successful_customer_ids)} 个客户添加到群组')
                            total_success += len(successful_customer_ids)
                        else:
                            self.logger.error(f'添加 {len(successful_customer_ids)} 个客户到群组失败')
                            total_failed += len(successful_customer_ids)
                else:
                    self.logger.error(f'第 {i//batch_size + 1} 批导入失败: {result.errors}')
                    total_failed += len(customers_dict)
                    total_pbar.update(len(customers_dict))
                
                batch_pbar.close()
            
            total_pbar.close()
            self.logger.info(f'导入完成: 成功 {total_success} 个, 失败 {total_failed} 个')
            return True, {'responses': all_responses}
        except Exception as e:
            self.logger.error(f'批量创建客户失败: {str(e)}')
            return False, str(e)

    def import_customers(self, file_path):
        """导入客户数据的主要流程"""
        customers = self.read_file(file_path)
        if not customers:
            self.logger.error('没有找到客户数据')
            return
        
        total = len(customers)
        success = 0
        failed = 0
        valid_customers = []
        
        # 创建客户组
        group_name = datetime.now().strftime('%y/%m/%d_%H:%M_自动导入')
        group_id = self.create_customer_group(group_name)
        if not group_id:
            self.logger.error('创建客户群组失败，导入过程终止')
            return
        
        # 创建验证进度条
        validate_pbar = tqdm(total=total, desc='验证进度', unit='客户')
        
        # 验证所有客户数据
        for customer in customers:
            if not self.validate_customer_data(customer):
                self.logger.warning(f'客户数据验证失败: {customer}')
                failed += 1
            else:
                valid_customers.append(customer)
                success += 1
            validate_pbar.update(1)
            validate_pbar.set_postfix({'成功': success, '失败': failed})
        
        validate_pbar.close()
        
        if valid_customers:
            self.logger.info(f'开始批量导入 {len(valid_customers)} 个有效客户...')
            is_success, result = self.create_customers_batch(valid_customers, group_id)
            
            if is_success:
                success = len(valid_customers)
                self.logger.info('批量导入成功')
            else:
                failed += len(valid_customers)
                self.logger.error(f'批量导入失败: {result}')
        
        self.logger.info(f'导入完成: 成功 {success} 个, 失败 {failed} 个')

    def create_customer_group(self, group_name):
        """创建客户群组
        
        Args:
            group_name: 群组名称
            
        Returns:
            成功返回群组ID，失败返回None
        """
        try:
            # 使用群组名称作为幂等性键的一部分，确保相同名称的群组不会重复创建
            idempotency_key = f"{group_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
            
            result = self.client.customer_groups.create_customer_group(
                body={
                    'idempotency_key': idempotency_key,
                    'group': {
                        'name': group_name
                    }
                }
            )
            
            if result.is_success():
                group_id = result.body.get('group', {}).get('id')
                self.logger.info(f'成功创建客户群组: {group_name} (ID: {group_id})')
                return group_id
            else:
                # 根据API文档，处理2022-03-16版本后的错误响应
                errors = result.errors
                if any(error.get('code') == 'BAD_REQUEST' for error in errors):
                    self.logger.error('创建群组请求包含无效字段')
                else:
                    self.logger.error(f'创建客户群组失败: {errors}')
                return None
        except Exception as e:
            self.logger.error(f'创建客户群组时发生错误: {str(e)}')
            return None

    def add_customers_to_group(self, group_id, customer_ids):
        """将客户添加到群组
        
        Args:
            group_id: 群组ID
            customer_ids: 要添加的客户ID列表
            
        Returns:
            添加成功返回True，失败返回False
        """
        if not group_id or not customer_ids:
            self.logger.error('群组ID或客户ID列表为空')
            return False
            
        try:
            # 分批处理客户，避免单次请求数据过大
            batch_size = 100
            success = True
            total_added = 0
            total_failed = 0
            
            # 创建总进度条
            total_pbar = tqdm(total=len(customer_ids), desc='添加客户到群组', unit='客户')
            
            for i in range(0, len(customer_ids), batch_size):
                batch_customer_ids = customer_ids[i:i + batch_size]
                # 创建批次进度条
                batch_pbar = tqdm(total=len(batch_customer_ids), desc=f'第 {i//batch_size + 1} 批', 
                                 unit='客户', leave=False)
                
                for customer_id in batch_customer_ids:
                    result = self.client.customers.add_group_to_customer(
                        customer_id=customer_id,
                        group_id=group_id
                    )
                    
                    if not result.is_success():
                        errors = result.errors
                        error_details = []
                        for error in errors:
                            if error.get('code') == 'NOT_FOUND':
                                error_details.append('群组或客户不存在')
                            elif error.get('code') == 'INVALID_REQUEST':
                                error_details.append('请求格式无效')
                            else:
                                error_details.append(str(error))
                        
                        self.logger.error(f'添加客户 {customer_id} 到群组失败: {", ".join(error_details)}')
                        total_failed += 1
                        success = False
                    else:
                        total_added += 1
                    
                    # 更新两个进度条
                    batch_pbar.update(1)
                    total_pbar.update(1)
                    # 更新进度条显示的成功/失败数量
                    batch_pbar.set_postfix({'成功': total_added, '失败': total_failed})
                    total_pbar.set_postfix({'成功': total_added, '失败': total_failed})
                
                batch_pbar.close()
            
            total_pbar.close()
            self.logger.info(f'添加客户到群组完成: 成功 {total_added} 个, 失败 {total_failed} 个')
            return success
        except Exception as e:
            self.logger.error(f'添加客户到群组时发生错误: {str(e)}')
            return False

def main():
    # 从环境变量获取Square访问令牌
    access_token = os.getenv('SQUARE_ACCESS_TOKEN')
    if not access_token:
        print('错误: 未设置SQUARE_ACCESS_TOKEN环境变量')
        sys.exit(1)
    
    while True:
        # 请求用户输入文件路径
        file_path = input('请输入客户数据文件路径 (支持 .csv, .xlsx, .xls 格式): ').strip()
        
        # 检查文件是否存在
        if not os.path.exists(file_path):
            print(f'错误: 文件 {file_path} 不存在')
            continue
        
        # 检查文件格式
        file_ext = os.path.splitext(file_path)[1].lower()
        if file_ext not in ['.csv', '.xlsx', '.xls']:
            print(f'错误: 不支持的文件格式 {file_ext}，仅支持 CSV 和 Excel 文件')
            continue
        
        # 文件验证通过，跳出循环
        break
    
    # 创建导入器实例并执行导入
    importer = SquareCustomerImport(access_token)
    importer.import_customers(file_path)

if __name__ == '__main__':
    main()