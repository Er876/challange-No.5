#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys, csv, configparser, queue
from multiprocessing import Queue, Process
from getopt import getopt, GetoptError
from datetime import datetime
from collections import namedtuple

# 税率表条目类
IncomeTaxQuickLookupItem = namedtuple(
    'IncomeTaxQuickLookupItem',
    ['start_point', 'tax_rate', 'quick_subtractor']
)

# 起征点常量
INCOME_TAX_START_POINT = 3500

# 税率表
INCOME_TAX_QUICK_LOOKUP_TABLE = [
    IncomeTaxQuickLookupItem(80000, 0.45, 13505),
    IncomeTaxQuickLookupItem(55000, 0.35, 5505),
    IncomeTaxQuickLookupItem(35000, 0.30, 2755),
    IncomeTaxQuickLookupItem(9000, 0.25, 1005),
    IncomeTaxQuickLookupItem(4500, 0.20, 555),
    IncomeTaxQuickLookupItem(1500, 0.1, 105),
    IncomeTaxQuickLookupItem(0, 0.03, 0)
]

# 命令行参数处理类
class Args(object):
    # 解析命令行选项
    def __init__(self):
        self.options = self._options()

    # 内部函数，用来解析命令行选项，返回保存了所有选项及其取值的字典
    def _options(self):
        try:
            opts, _ = getopt(sys.argv[1:], 'hC:c:d:o:', ['help'])
        except GetoptError:
            print('Parameter Error')
            exit()
        options = dict(opts)

        # 处理 -h 或 --help 选项
        if len(options) == 1 and ('-h' in options or '--help' in options):
            print(
                'Usage: calculator.py -C cityname -c configfile -d userdata -o resultdata')
            exit()

        return options

    def _value_after_option(self, option):
        value = self.options.get(option)
        if value is None and option != '-C':
            print("Parameter Error")
            exit()
        return value

    # 城市参数为可选项
    @property
    def city(self):
        return self._value_after_option('-C')

    # 配置文件路径
    @property
    def config_path(self):
        return self._value_after_option('-c')

    # 用户工资文件路径
    @property
    def userdata_path(self):
        return self._value_after_option('-d')

    # 税后工资文件路径
    @property
    def export_path(self):
        return self._value_after_option('-o')

args = Args()

# 配置文件处理类
class Config(object):
    def __init__(self):
        self.config = self._read_config()

    def _read_config(self):
        config = configparser.ConfigParser()
        config.read(args.config_path)
        # 如果指定了城市并且该城市在配置文件中，返回该城市的配置，否则返回默认的配置
        if args.city and args.city.upper() in config.sections():
            return config[args.city.upper()]
        else:
            return config['DEFAULT']

    def _get_config(self, key):
        try:
            return float(self.config[key])
        except (ValueError, KeyError):
            print("Parameter Error")
            exit()

    @property
    # 获取社保基数下限
    def social_insurance_baseline_low(self):
        return self._get_config('JiShuL')

    # 获取社保基数上限
    @property
    def social_insurance_baseline_high(self):
        return self._get_config('JiShuH')

    # 获取社保总费率
    @property
    def social_insurance_total_rate(self):
        return sum([
            self._get_config('YangLao'),
            self._get_config('YiLiao'),
            self._get_config('ShiYe'),
            self._get_config('GongShang'),
            self._get_config('ShengYu'),
            self._get_config('GongJiJin')
        ])

config = Config()

# 用户工资文件处理进程
class UserData(Process):
    def __init__(self, userdata_queue):
        super().__init__()
        # 用户数据队列
        self.userdata_queue = userdata_queue

    def _read_users_data(self):
        userdata = []
        with open(args.userdata_path) as f:
            for line in f.readlines():
                employee_id, income_string = line.strip().split(',')
                try:
                    income = int(income_string)
                except ValueError:
                    print('Parameter Error')
                    exit()
                userdata.append((employee_id, income))
        return userdata

    def run(self):
    # 从用户数据文件依次读取每条用户数据并写入队列
        for item in self._read_users_data():
            self.userdata_queue.put(item)

# 税后工资计算进程
class IncomeTaxCalculator(Process):
    def __init__(self, userdata_queue, export_queue):
        super().__init__()

        # 用户数据队列
        self.userdata_queue = userdata_queue
        # 导出数据队列
        self.export_queue = export_queue

    # 计算应纳税额
    @staticmethod
    def calc_social_insurance_money(income):
        if income < config.social_insurance_baseline_low:
            return config.social_insurance_baseline_low * \
                config.social_insurance_total_rate
        elif income > config.social_insurance_baseline_high:
            return config.social_insurance_baseline_high * \
                config.social_insurance_total_rate
        else:
            return income * config.social_insurance_total_rate

    # 计算税后工资
    @classmethod
    def calc_income_tax_and_remain(cls, income):
        # 计算社保金额
        social_insurance_money = cls.calc_social_insurance_money(income)

        # 计算应纳税额
        real_income = income - social_insurance_money
        taxable_part = real_income - INCOME_TAX_START_POINT

        for item in INCOME_TAX_QUICK_LOOKUP_TABLE:
            if taxable_part > item.start_point:
                tax = taxable_part * item.tax_rate - item.quick_subtractor
                return '{:.2f}'.format(tax), '{:.2f}'.format(real_income - tax)
        return '0.00', '{:.2f}'.format(real_income)

    def calculate(self, employee_id, income):
        # 计算社保金额
        social_insurance_money = '{:.2f}'.format(
            self.calc_social_insurance_money(income))

        tax, remain = self.calc_income_tax_and_remain(income)

        return [employee_id, income, social_insurance_money, tax, remain,
                datetime.now().strftime('%Y-%m-%d %H:%M:%S')]

    # 进程入口方法
    def run(self):
        while True:
            try:
                employee_id, income = self.userdata_queue.get(timeout=1)
            except queue.Empty:
                return

            # 计算税后工资
            result = self.calculate(employee_id, income)

            self.export_queue.put(result)

# 税后工资导出进程
class IncomeTaxExporter(Process):
    def __init__(self, export_queue):
        super().__init__()

        # 导出数据队列
        self.export_queue = export_queue

        # 创建 CSV 写入器
        self.file = open(args.export_path, 'w', newline='')
        self.writer = csv.writer(self.file)

    def run(self):
        # 从导出数据队列读取导出数据，写入到导出文件中
        while True:
            try:
                item = self.export_queue.get(timeout=1)
            except queue.Empty:
                self.file.close()
                return

            self.writer.writerow(item)

if __name__ == '__main__':
    # 创建进程之间通信的队列
    userdata_queue = Queue()
    export_queue = Queue()

    # 用户数据进程
    userdata = UserData(userdata_queue)
    # 税后工资计算进程
    calculator = IncomeTaxCalculator(userdata_queue, export_queue)
    # 税后工资导出进程
    exporter = IncomeTaxExporter(export_queue)

    # 启动进程
    userdata.start()
    calculator.start()
    exporter.start()

    userdata.join()
    calculator.join()
    exporter.join()
