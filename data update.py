import os
import re
from typing import Optional

import pandas as pd


def find_id_column(df_columns: pd.Index) -> Optional[str]:
    """在一个 DataFrame 的列中寻找 BGM 的 ID 列。"""
    # 按照您之前确认的优先级和名称查找
    for col in ['bgmid', 'bgm_id', 'id', '游戏ID']:
        if col in df_columns:
            return col
    return None


def find_alias_column(df_columns: pd.Index) -> Optional[str]:
    """在一个 DataFrame 的列中寻找别名列。"""
    for col in ['别名', 'bgm游戏', 'alias', 'aliases']:
        if col in df_columns:
            return col
    return None


def update_aliases_from_source(
        main_file: str = '主表.xlsx',
        alias_source_file: str = r"E:\学习资料\项目文件\BaiduTiebaSpider-main\ymproject2\bgm_archive_20250525 (1).xlsx",
        output_file_suffix: str = '_updated'
) -> None:
    """
    使用 bgm_archive 文件中的别名信息来更新主表。

    该函数会：
    1. 读取主表和别名源文件。
    2. 根据共享的 ID 列匹配两个表的数据。
    3. 移除主表中所有以 '别名' 开头的旧列。
    4. 将源文件中的别名（以'|'分隔）拆分成新的 '别名1', '别名2', ... 列。
    5. 将更新后的数据保存到一个新文件（默认为 主表_updated.xlsx）。

    Args:
        main_file (str): 待更新的主表 Excel 文件路径。
        alias_source_file (str): 提供别名来源的 Excel 文件路径。
        output_file_suffix (str): 添加到输出文件名中的后缀。
    """
    print("--- 开始执行别名更新流程 ---")
    try:
        print(f"正在读取主表: {main_file}")
        main_df = pd.read_excel(main_file)
        print(f"正在读取别名源文件: {alias_source_file}")
        alias_df = pd.read_excel(alias_source_file)
    except FileNotFoundError as e:
        print(f"错误: 文件未找到 - {e}")
        return

    # 1. 自动查找两个表中的 ID 列
    main_id_col = find_id_column(main_df.columns)
    alias_id_col = find_id_column(alias_df.columns)
    alias_col_name = find_alias_column(alias_df.columns)

    if not main_id_col or not alias_id_col:
        print("错误: 未能在两个文件中都找到有效的 ID 列 (如 'bgmid', 'id' 等)。")
        return
    
    if not alias_col_name:
        print("错误: 别名源文件中未找到有效的别名列 (如 '别名', 'bgm游戏' 等)。")
        return

    print(f"主表ID列: '{main_id_col}', 源文件ID列: '{alias_id_col}', 源文件别名列: '{alias_col_name}'")

    # 2. 准备合并数据
    # 选择ID和别名列，重命名以防冲突
    alias_source = alias_df[[alias_id_col, alias_col_name]].copy()
    alias_source.rename(columns={alias_col_name: 'source_aliases'}, inplace=True)
    # 确保源ID不为空，并将ID列转为字符串以保证匹配可靠性
    alias_source.dropna(subset=[alias_id_col], inplace=True)
    alias_source[alias_id_col] = alias_source[alias_id_col].astype(str)
    main_df[main_id_col] = main_df[main_id_col].astype(str)


    # 3. 清理主表中已有的别名列
    existing_alias_cols = [col for col in main_df.columns if str(col).startswith('别名')]
    if existing_alias_cols:
        print(f"正在从主表移除旧的别名列: {existing_alias_cols}")
        main_df.drop(columns=existing_alias_cols, inplace=True)

    # 4. 合并主表与新的别名源
    updated_df = pd.merge(main_df, alias_source, left_on=main_id_col, right_on=alias_id_col, how='left')
    
    # 5. 将源别名字符串拆分成单独的列
    # 使用正则表达式来处理多种可能的分隔符（, | ， ; : ： 等）
    aliases_series = updated_df['source_aliases'].fillna('').apply(
        lambda x: [alias.strip() for alias in re.split(r'[、,，;:：|]', str(x)) if alias.strip()]
    )
    
    # 只有在确实有别名需要处理时才继续
    if aliases_series.apply(len).any():
        alias_df_new = pd.DataFrame(aliases_series.tolist())
        # 为新别名列命名为 别名1, 别名2, ...
        alias_df_new.columns = [f'别名{i+1}' for i in range(alias_df_new.shape[1])]
        
        # 6. 将新别名列合并回主 DataFrame
        final_df = pd.concat([updated_df, alias_df_new], axis=1)
    
        # 7. 清理辅助列并保存
        final_df.drop(columns=['source_aliases'], inplace=True, errors='ignore')
        # 如果 merge 产生了重复的 ID 列，也一并清理
        if alias_id_col != main_id_col and alias_id_col in final_df.columns:
             final_df.drop(columns=[alias_id_col], inplace=True, errors='ignore')
    else:
        print("警告: 源文件中的别名列为空或格式不正确，未生成新的别名列。")
        final_df = updated_df.drop(columns=['source_aliases'], errors='ignore')

             
    base, ext = os.path.splitext(main_file)
    output_file = f"{base}{output_file_suffix}{ext}"

    print(f"别名更新完成，正在保存到: {output_file}")
    final_df.to_excel(output_file, index=False)
    print("保存成功！🎉")

    print("--- 别名更新流程结束 ---")


if __name__ == "__main__":
    # 该脚本可直接运行，用于更新主表的别名
    update_aliases_from_source()
