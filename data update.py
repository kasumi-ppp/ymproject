import pandas as pd
import os

def normalize_id_series(series: pd.Series) -> pd.Series:
    """
    将ID列标准化为字符串，去除 '.0' 后缀。
    这能确保 '123' 和 '123.0' 可以互相匹配。
    """
    return series.astype(str).str.replace(r'\.0$', '', regex=True)

def update_aliases_simplified(
    main_file: str = '主表.xlsx',
    alias_source_file: str = 'bgm_archive_20250525 (1).xlsx',
    output_file_suffix: str = '_updated'
):
    """
    一个更简洁、直接的别名更新脚本。
    1. 读取主表和别名来源文件。
    2. 将两个文件的ID列统一为字符串类型，以确保准确匹配。
    3. 从主表中移除所有旧的'别名'列。
    4. 将别名来源文件中的'id'和'别名'列合并到主表。
    5. 保存为新文件。
    """
    print("--- 开始执行简化版别名更新流程 ---")

    try:
        main_df = pd.read_excel(main_file)
        print(f"成功读取主表: {main_file}")
        alias_df = pd.read_excel(alias_source_file)
        print(f"成功读取别名源文件: {alias_source_file}")
    except FileNotFoundError as e:
        print(f"错误: 文件未找到 - {e}")
        return

    # 定义ID列和别名列
    main_id_col = 'bgmid'
    alias_id_col = 'id'
    alias_col_name = '别名'

    # 检查必要的列是否存在
    if main_id_col not in main_df.columns:
        print(f"错误: 主表 '{main_file}' 中缺少 '{main_id_col}' 列。")
        return
    if alias_id_col not in alias_df.columns:
        print(f"错误: 别名源文件 '{alias_source_file}' 中缺少 '{alias_id_col}' 列。")
        return
    if alias_col_name not in alias_df.columns:
        print(f"错误: 别名源文件 '{alias_source_file}' 中缺少 '{alias_col_name}' 列。")
        return

    # --- 核心逻辑 ---

    # 1. 标准化ID列，确保可以匹配
    print("正在标准化ID列...")
    main_df[main_id_col] = normalize_id_series(main_df[main_id_col])
    alias_df[alias_id_col] = normalize_id_series(alias_df[alias_id_col])

    # 2. 从主表中删除所有以'别名'开头的列
    existing_alias_cols = [col for col in main_df.columns if str(col).startswith('别名')]
    if existing_alias_cols:
        print(f"正在从主表移除旧的别名列: {existing_alias_cols}")
        main_df = main_df.drop(columns=existing_alias_cols)

    # 3. 准备要合并的别名数据，只保留ID和别名列
    alias_to_merge = alias_df[[alias_id_col, alias_col_name]].copy()
    alias_to_merge.dropna(subset=[alias_col_name], inplace=True)
    
    # 4. 使用 left merge 将别名合并到主表
    print(f"开始根据 '{main_id_col}' (主表) 和 '{alias_id_col}' (源文件) 进行合并...")
    updated_df = pd.merge(
        main_df,
        alias_to_merge,
        left_on=main_id_col,
        right_on=alias_id_col,
        how='left'
    )
    
    # 合并后，源文件的列名会 그대로 가져옴. 我们将它重命名为 '别名'
    if alias_col_name in updated_df.columns:
        updated_df.rename(columns={alias_col_name: '别名'}, inplace=True)
    
    # 删除多余的ID列
    if alias_id_col != main_id_col and alias_id_col in updated_df.columns:
        updated_df = updated_df.drop(columns=[alias_id_col])

    # --- 检查合并结果 ---
    # 检查 '别名' 列是否存在
    if '别名' in updated_df.columns:
        num_matched = updated_df['别名'].notna().sum()
        total_rows = len(updated_df)
        print(f"合并完成。总共 {total_rows} 行，成功匹配并更新了 {num_matched} 行的别名。")
        if num_matched == 0:
            print("警告：没有一行匹配成功。请检查两个文件的ID是否能够对应。")
    else:
        print("错误：合并后 '别名' 列不存在。")

    # 5. 保存结果
    base, ext = os.path.splitext(main_file)
    output_file = f"{base}{output_file_suffix}{ext}"
    
    try:
        updated_df.to_excel(output_file, index=False)
        print(f"保存成功！🎉 文件已保存至: {output_file}")
    except Exception as e:
        print(f"保存文件时出错: {e}")

    print("--- 流程结束 ---")


if __name__ == "__main__":
    update_aliases_simplified()
