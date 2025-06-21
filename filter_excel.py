import pandas as pd
from datetime import datetime
import os
import argparse
import re
import sys

def filter_excel_by_score(input_file: str, score_threshold: float):
    """
    筛选Excel文件，保留score得分大于指定阈值的行。
    """
    print(f"正在读取文件: {input_file}")
    try:
        df = pd.read_excel(input_file)
    except FileNotFoundError:
        print(f"错误: 文件 '{input_file}' 未找到。请确保文件路径正确。")
        return
    except Exception as e:
        print(f"读取Excel文件时出错: {e}")
        return

    if 'score' not in df.columns:
        print("错误: Excel文件中未找到 'score' 列。请检查列名。")
        return

    print(f"原始数据包含 {len(df)} 行。")
    # 过滤得分大于阈值的行
    print(f"正在筛选 'score' > {score_threshold} 的行...")
    filtered_df = df[df['score'] > score_threshold].copy() # 使用 .copy() 避免 SettingWithCopyWarning

    if filtered_df.empty:
        print(f"没有找到 'score' 大于 {score_threshold} 的数据。")
        return

    # 在filtered_df上直接操作，修改数据类型
    # filtered_df['orgId'] = filtered_df['orgId'].astype(str)
    # filtered_df['ym_id'] = filtered_df['ym_id'].astype(str)


    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    # 更新输出文件名以反映新的阈值
    output_file = f"ymgames_matched_filtered_score_gt_{str(score_threshold).replace('.', '_')}_{timestamp}.xlsx"

    try:
        filtered_df.to_excel(output_file, index=False)
        print(f"已将 {len(filtered_df)} 行数据保存到: {output_file}")
    except Exception as e:
        print(f"保存筛选结果时出错: {e}")


def process_and_filter_for_aliases(input_file: str, score_limit: float):
    """
    处理指定Excel文件：
    1. 筛选出'别名'列存在内容且'score' <= score_limit 的行。
    2. 使用正则表达式拆分'别名'列。
    3. 保存到新文件。
    """
    print(f"--- 开始处理别名并筛选文件: {input_file} ---")
    
    try:
        df = pd.read_excel(input_file)
        print(f"成功读取文件，包含 {len(df)} 行。")
    except FileNotFoundError:
        print(f"错误: 文件 '{input_file}' 未找到。")
        return
    except Exception as e:
        print(f"读取Excel文件时出错: {e}")
        return

    # 检查必需的列
    required_cols = ['别名', 'score']
    if not all(col in df.columns for col in required_cols):
        print(f"错误: 文件中必须包含 {required_cols} 列。当前列: {df.columns.tolist()}")
        return

    print("\n--- Debug: 原始数据诊断 ---")
    print("列的数据类型:")
    df.info(verbose=False)
    print("\n'score'列的统计信息:")
    print(df['score'].describe())
    print(f"\n'别名'列非空值的数量: {df['别名'].notna().sum()}")

    # 1. 筛选
    print(f"\n--- 正在筛选 ---")
    print(f"筛选条件: '别名'列不为空 且 'score' <= {score_limit}")
    
    # 筛选条件1: 别名列存在且不为空字符串
    df_with_alias = df[df['别名'].notna() & (df['别名'].astype(str).str.strip() != '')].copy()
    print(f"Debug: 步骤1 (筛选有别名的行) 后剩下: {len(df_with_alias)} 行")
    
    # 在继续之前，确保score是数字类型
    df_with_alias['score'] = pd.to_numeric(df_with_alias['score'], errors='coerce')
    
    # 筛选条件2: score <= 0.9
    filtered_df = df_with_alias[df_with_alias['score'] <= score_limit].copy()
    print(f"Debug: 步骤2 (在有别名的基础上，筛选 score <= {score_limit}) 后剩下: {len(filtered_df)} 行")

    if filtered_df.empty:
        print("\n--- 筛选失败：没有找到任何符合条件的数据 ---")
        # 诊断问题所在
        alias_condition_count = (df['别名'].notna() & (df['别名'].astype(str).str.strip() != '')).sum()
        if alias_condition_count == 0:
            print(f"\n[!!] 错误原因: 您提供的输入文件 ('{input_file}') 中，'别名' 列是完全空的。")
            print("     脚本无法找到任何可以拆分的数据。")
            print("\n[>>] 解决方案: 请使用包含了别名数据的文件。例如，您之前生成的 '主表_updated.xlsx'。")
            print("     您可以尝试运行以下完整命令:")
            print(f"\n     python filter_excel.py process_alias 主表_updated.xlsx\n")
        else:
             # 如果别名存在，但分数不匹配
            numeric_score = pd.to_numeric(df['score'], errors='coerce')
            score_condition_count = (numeric_score <= score_limit).sum()
            print(f"\n[!!] 诊断信息: 文件中有 {alias_condition_count} 行包含别名，但没有一行的 'score' 值小于或等于 {score_limit}。")
            print(f"     (文件中 'score' <= {score_limit} 的总行数为: {score_condition_count})")
        return
    
    print(f"筛选成功，找到 {len(filtered_df)} 行。")

    # 2. 拆分别名列
    print("\n正在拆分 '别名' 列...")
    
    # 使用正则表达式来处理多种可能的分隔符
    aliases_series = filtered_df['别名'].apply(
        lambda x: [alias.strip() for alias in re.split(r'[、,，;:：|]', str(x)) if alias.strip()]
    )
    
    if aliases_series.empty or aliases_series.apply(len).sum() == 0:
        print("警告：'别名'列在筛选后为空或无法拆分，将只保存筛选结果。")
        final_df = filtered_df
    else:
        # 创建新的别名DataFrame
        alias_df_new = pd.DataFrame(aliases_series.tolist(), index=filtered_df.index)
        # 为新别名列命名
        alias_df_new.columns = [f'别名{i+1}' for i in range(alias_df_new.shape[1])]
        # 合并回主DataFrame，并删除原始的'别名'列
        final_df = pd.concat([filtered_df.drop('别名', axis=1), alias_df_new], axis=1)
        print(f"成功将'别名'拆分为 {alias_df_new.shape[1]} 个新列。")


    # 3. 保存结果
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_name = os.path.basename(input_file).split('.')[0]
    output_file = f"{base_name}_processed_aliases_{timestamp}.xlsx"

    try:
        final_df.to_excel(output_file, index=False)
        print(f"处理完成！已将 {len(final_df)} 行数据保存到: {output_file}")
    except Exception as e:
        print(f"保存结果时出错: {e}")


if __name__ == "__main__":
    # 如果用户没有提供任何命令行参数，直接运行默认的"处理别名"任务
    if len(sys.argv) == 1:
        print("--- 未提供任何命令行参数，将以默认设置执行'处理别名'任务 ---")
        process_and_filter_for_aliases(
            input_file="主表_updated.xlsx",
            score_limit=0.9
        )
    else:
        # 否则，使用我们之前定义的、更灵活的命令行解析逻辑
        parser = argparse.ArgumentParser(
            description="筛选或处理 Excel 文件。有两个任务可选：'filter' 和 'process_alias'。",
            formatter_class=argparse.RawTextHelpFormatter
        )
        
        subparsers = parser.add_subparsers(dest='task', help="选择要执行的任务。例如: 'python filter_excel.py filter --help'", required=True)

        # 任务1: filter - 根据 score > threshold 筛选
        parser_filter = subparsers.add_parser('filter', help="根据 score > threshold 筛选行 (旧功能)")
        parser_filter.add_argument(
            "input_file",
            nargs='?',
            default="ymgames_matched_new.xlsx",
            help="要筛选的Excel文件。(默认: ymgames_matched_new.xlsx)"
        )
        parser_filter.add_argument(
            "--threshold",
            type=float,
            default=0.5,
            help="分数的阈值，只保留大于此值的行。(默认: 0.5)"
        )

        # 任务2: process_alias - 筛选并拆分别名
        parser_alias = subparsers.add_parser('process_alias', help="筛选(score<=上限, 有别名)并拆分别名列 (新功能)")
        parser_alias.add_argument(
            "input_file",
            nargs='?',
            default="拆分主表_updated_20250615_184956.xlsx",
            help="要处理的Excel文件。(默认: 拆分主表_updated_20250615_184956.xlsx)"
        )
        parser_alias.add_argument(
            "--score_limit",
            type=float,
            default=0.9,
            help="分数的上限，只保留小于或等于此值的行。(默认: 0.9)"
        )

        args = parser.parse_args()

        if args.task == 'filter':
            filter_excel_by_score(input_file=args.input_file, score_threshold=args.threshold)
        elif args.task == 'process_alias':
            process_and_filter_for_aliases(input_file=args.input_file, score_limit=args.score_limit) 