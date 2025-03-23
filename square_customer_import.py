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
        
        # 获取环境变量中的日志级别，默认为INFO
        log_level_name = os.getenv('LOG_LEVEL', 'INFO')
        log_level = getattr(logging, log_level_name, logging.INFO)
        
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(f'logs/import_log_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        self.logger.info(f'日志级别设置为: {log_level_name}')
    
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
        total_records = 0
        success_week_parse = 0
        failed_week_parse = 0
        try:
            file_ext = os.path.splitext(file_path)[1].lower()
            self.logger.info(f'开始读取{file_ext}格式文件: {file_path}')
            
            if file_ext == '.csv':
                with open(file_path, 'r', encoding='utf-8') as file:
                    reader = csv.DictReader(file)
                    for row_index, row in enumerate(reader, 1):
                        total_records += 1
                        # 处理客户姓名
                        name_parts = self.process_name(str(row.get('Customer name', '')))
                        row['given_name'] = name_parts['given_name']
                        row['family_name'] = name_parts['family_name']
                        
                        # 处理电话号码
                        if 'Customer phone number' in row:
                            row['phone_number'] = self.format_phone_number(row.get('Customer phone number', ''))
                        
                        # 处理Pick-up time并添加周数信息
                        if 'Pick-up time (local)' in row and row['Pick-up time (local)']:
                            try:
                                pickup_time_str = str(row['Pick-up time (local)']).strip()
                                pickup_time = datetime.strptime(pickup_time_str, '%Y-%m-%d %H:%M:%S')
                                # 获取ISO周数（1-53）
                                week_number = pickup_time.isocalendar()[1]
                                row['week_number'] = week_number
                                success_week_parse += 1
                                self.logger.debug(f"记录 {row_index}: 成功解析Pick-up time '{pickup_time_str}', 周数={week_number}")
                            except (ValueError, TypeError) as e:
                                self.logger.warning(f"记录 {row_index}: 无法解析Pick-up time: '{row.get('Pick-up time (local)', '')}', 错误: {str(e)}")
                                row['week_number'] = 0
                                failed_week_parse += 1
                        else:
                            self.logger.warning(f"记录 {row_index}: 缺少Pick-up time字段或值为空")
                            row['week_number'] = 0
                            failed_week_parse += 1
                        customers.append(row)
            elif file_ext in ['.xlsx', '.xls']:
                df = pd.read_excel(file_path)
                total_records = len(df)
                self.logger.info(f'Excel文件共有 {total_records} 条记录')
                
                for row_index, row_data in enumerate(df.iterrows(), 1):
                    _, row = row_data  # pandas返回(index, Series)元组
                    name_parts = self.process_name(str(row.get('Customer name', '')))
                    customer = {
                        'given_name': name_parts['given_name'],
                        'family_name': name_parts['family_name'],
                        'email_address': str(row.get('Customer email', '')),
                        'phone_number': self.format_phone_number(row.get('Customer phone number', ''))
                    }
                    
                    # 处理Pick-up time并添加周数信息
                    if 'Pick-up time (local)' in row and not pd.isna(row['Pick-up time (local)']):
                        try:
                            pickup_time_str = str(row['Pick-up time (local)']).strip()
                            pickup_time = datetime.strptime(pickup_time_str, '%Y-%m-%d %H:%M:%S')
                            # 获取ISO周数（1-53）
                            week_number = pickup_time.isocalendar()[1]
                            customer['week_number'] = week_number
                            success_week_parse += 1
                            self.logger.debug(f"记录 {row_index}: 成功解析Pick-up time '{pickup_time_str}', 周数={week_number}")
                        except (ValueError, TypeError) as e:
                            self.logger.warning(f"记录 {row_index}: 无法解析Pick-up time: '{row.get('Pick-up time (local)', '')}', 错误: {str(e)}")
                            customer['week_number'] = 0
                            failed_week_parse += 1
                    else:
                        self.logger.warning(f"记录 {row_index}: 缺少Pick-up time字段或值为空")
                        customer['week_number'] = 0
                        failed_week_parse += 1
                    
                    customers.append(customer)
            else:
                raise ValueError(f'不支持的文件格式: {file_ext}')
                
            self.logger.info(f'文件读取完成: 总记录数={total_records}, 成功解析周数={success_week_parse}, 失败={failed_week_parse}')
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
            batch_size = 100
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

    def check_duplicate_in_group(self, phone_number, group_customers):
        """检查手机号在同一周客户组内是否重复
        
        Args:
            phone_number: 要检查的手机号
            group_customers: 同一周的客户列表
            
        Returns:
            如果手机号在组内重复返回True，否则返回False
        """
        if not phone_number or not group_customers:
            return False
        
        self.logger.debug(f'检查手机号 {phone_number} 是否在组内重复')
            
        # 遍历组内所有客户，检查手机号是否重复
        for customer in group_customers:
            customer_phone = customer.get('phone_number')
            if customer_phone and customer_phone == phone_number:
                self.logger.debug(f'发现重复手机号: {phone_number}')
                return True
        return False
    
    def get_customers_in_group(self, group_id):
        """获取客户组内的所有客户
        
        Args:
            group_id: 客户组ID
            
        Returns:
            客户组内的客户列表，如果出错则返回空列表
        """
        try:
            self.logger.info(f'正在获取客户组 {group_id} 内的客户...')
            all_customers = []
            cursor = None
            page_limit = 100  # Square API限制每页最多100条记录
            
            # 使用分页查询获取所有客户
            while True:
                # 构建查询
                query = {
                    'query': {
                        'filter': {
                            'group_ids': {'any': [group_id]}
                        }
                    },
                    'limit': page_limit
                }
                
                # 添加游标用于分页
                if cursor:
                    query['cursor'] = cursor
                
                result = self.client.customers.search_customers(body=query)
                
                if result.is_success():
                    page_customers = result.body.get('customers', [])
                    all_customers.extend(page_customers)
                    self.logger.info(f'已获取 {len(all_customers)} 个客户')
                    
                    # 检查是否有更多页
                    cursor = result.body.get('cursor')
                    if not cursor:
                        break
                else:
                    self.logger.error(f'获取客户组内客户失败: {result.errors}')
                    return []
            
            self.logger.info(f'成功获取客户组内的 {len(all_customers)} 个客户')
            return all_customers
        except Exception as e:
            self.logger.error(f'获取客户组内客户时发生错误: {str(e)}')
            return []
    
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
        
        self.logger.info(f'开始验证{total}个客户数据...')
        
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
            # 按周数对客户进行分组
            customers_by_week = {}
            week_stats = {}
            
            self.logger.info('开始按周数分组客户数据...')
            for customer in valid_customers:
                week_number = customer.get('week_number', 0)
                if week_number not in customers_by_week:
                    customers_by_week[week_number] = []
                    week_stats[week_number] = {'total': 0, 'valid_week': 0, 'invalid_week': 0}
                
                customers_by_week[week_number].append(customer)
                week_stats[week_number]['total'] += 1
                
                if week_number > 0:
                    week_stats[week_number]['valid_week'] += 1
                else:
                    week_stats[week_number]['invalid_week'] += 1
            
            # 输出每个周的统计信息
            for week_number, stats in week_stats.items():
                if week_number == 0:
                    week_name = "未知周数"
                else:
                    week_name = f"第{week_number}周"
                    
                self.logger.info(f'{week_name}客户统计: 总数={stats["total"]}, 有效周数={stats["valid_week"]}, 无效周数={stats["invalid_week"]}')
            
            total_success = 0
            total_failed = 0
            
            # 为每个周创建一个客户组并导入客户
            for week_number, week_customers in customers_by_week.items():
                if week_number == 0:
                    group_name = "未知周数_客户组"
                else:
                    current_year = datetime.now().year
                    group_name = f"{current_year}年第{week_number}周_客户组"
                
                self.logger.info(f'为{group_name}的{len(week_customers)}个客户创建群组...')
                group_id = self.create_customer_group(group_name)
                
                if not group_id:
                    self.logger.error(f'创建{group_name}客户群组失败，跳过该批次')
                    total_failed += len(week_customers)
                    continue
                
                # 获取组内现有客户，用于手机号查重
                existing_customers = self.get_customers_in_group(group_id)
                self.logger.info(f'获取到{group_name}客户组内现有客户 {len(existing_customers)} 个')
                
                # 在同一周内进行手机号查重
                deduplicated_customers = []
                duplicate_count = 0
                processed_phones = set()
                
                # 将现有客户的手机号添加到已处理集合中
                for existing_customer in existing_customers:
                    phone = existing_customer.get('phone_number')
                    if phone:
                        processed_phones.add(phone)
                        self.logger.debug(f'添加现有客户手机号到查重集合: {phone}')
                
                for customer in week_customers:
                    phone_number = customer.get('phone_number')
                    if phone_number and phone_number in processed_phones:
                        self.logger.warning(f'在{group_name}内发现重复手机号: {phone_number}，跳过该客户')
                        duplicate_count += 1
                        continue
                    
                    if phone_number:
                        processed_phones.add(phone_number)
                    deduplicated_customers.append(customer)
                
                if duplicate_count > 0:
                    self.logger.info(f'{group_name}内检测到{duplicate_count}个重复手机号客户，已跳过')
                
                self.logger.info(f'开始批量导入{group_name}的{len(deduplicated_customers)}个客户...')
                is_success, result = self.create_customers_batch(deduplicated_customers, group_id)
                
                if is_success:
                    total_success += len(deduplicated_customers)
                    self.logger.info(f'{group_name}客户批量导入成功')
                else:
                    total_failed += len(deduplicated_customers)
                    self.logger.error(f'{group_name}客户批量导入失败: {result}')
            
            success = total_success
            failed = total_failed
            
            # 添加每周导入数据的详细统计
            self.logger.info('='*50)
            self.logger.info('每周导入数据统计汇总:')
            self.logger.info('-'*50)
            
            # 创建每周统计数据字典
            week_import_stats = {}
            for week_number in customers_by_week.keys():
                week_import_stats[week_number] = {
                    'total': len(customers_by_week[week_number]),
                    'success': 0,
                    'skipped': 0
                }
            
            # 遍历每个周的统计信息
            for week_number, stats in sorted(week_import_stats.items()):
                if week_number == 0:
                    week_name = "未知周数"
                else:
                    week_name = f"第{week_number}周"
                
                # 计算成功和跳过的数量
                total_in_week = stats['total']
                skipped_in_week = 0
                
                # 获取该周的客户组名称
                if week_number == 0:
                    group_name = "未知周数_客户组"
                else:
                    current_year = datetime.now().year
                    group_name = f"{current_year}年第{week_number}周_客户组"
                
                # 查找该周的重复数据数量
                for week_num, week_customers in customers_by_week.items():
                    if week_num == week_number:
                        # 计算该周内被跳过的重复客户数量
                        original_count = len(week_customers)
                        # 获取该周的去重后客户数量
                        deduplicated_count = 0
                        processed_phones = set()
                        
                        # 模拟去重过程来计算跳过的数量
                        for customer in week_customers:
                            phone_number = customer.get('phone_number')
                            if phone_number and phone_number in processed_phones:
                                skipped_in_week += 1
                            else:
                                if phone_number:
                                    processed_phones.add(phone_number)
                                deduplicated_count += 1
                
                # 计算成功导入的数量
                success_in_week = total_in_week - skipped_in_week
                
                # 更新统计信息
                week_import_stats[week_number]['success'] = success_in_week
                week_import_stats[week_number]['skipped'] = skipped_in_week
                
                # 输出该周的统计信息
                self.logger.info(f'{week_name}客户导入统计: 总数={total_in_week}, 成功导入={success_in_week}, 跳过重复={skipped_in_week}')
            
            self.logger.info('-'*50)
            self.logger.info(f'总计: 成功导入 {success} 个, 失败 {failed} 个')
            self.logger.info('='*50)
        
        self.logger.info(f'导入完成: 成功 {success} 个, 失败 {failed} 个')

    def create_customer_group(self, group_name):
        """创建客户群组
        
        Args:
            group_name: 群组名称
            
        Returns:
            成功返回群组ID，失败返回None
        """
        try:
            # 检查是否已存在同名群组
            existing_group_id = self.find_customer_group_by_name(group_name)
            if existing_group_id:
                self.logger.info(f'找到已存在的客户群组: {group_name} (ID: {existing_group_id})')
                return existing_group_id
            
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
            
    def find_customer_group_by_name(self, group_name):
        """根据名称查找客户群组
        
        Args:
            group_name: 群组名称
            
        Returns:
            找到返回群组ID，未找到返回None
        """
        try:
            result = self.client.customer_groups.list_customer_groups()
            
            if result.is_success():
                groups = result.body.get('groups', [])
                for group in groups:
                    if group.get('name') == group_name:
                        return group.get('id')
            return None
        except Exception as e:
            self.logger.warning(f'查找客户群组时发生错误: {str(e)}')
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
    # 根据环境获取对应的访问令牌
    environment = os.getenv('SQUARE_ENVIRONMENT', 'sandbox')
    if environment == 'sandbox':
        access_token = os.getenv('SQUARE_SANDBOX_ACCESS_TOKEN')
    else:
        access_token = os.getenv('SQUARE_PRODUCTION_ACCESS_TOKEN')
    
    if not access_token:
        print(f'错误: 未设置{environment}环境的访问令牌')
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