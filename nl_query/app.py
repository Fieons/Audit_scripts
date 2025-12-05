"""
Streamlit主应用
自然语言SQL查询工具的Web界面
"""

import streamlit as st
import pandas as pd
import time
from typing import Optional, Dict, Any
import logging

from config import validate_config, get_config_summary
from sql_generator import SQLGenerator, SQLGenerationError
from utils import (
    setup_logging, format_error_message, format_sql_for_display,
    format_dataframe_for_display, create_example_queries,
    validate_natural_language_query, QueryHistory
)

# 页面配置
st.set_page_config(
    page_title="审计凭证自然语言查询工具",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 初始化日志
setup_logging()

# 初始化会话状态
def init_session_state():
    """初始化会话状态"""
    if 'sql_generator' not in st.session_state:
        st.session_state.sql_generator = None
    if 'query_history' not in st.session_state:
        st.session_state.query_history = QueryHistory()
    if 'current_query' not in st.session_state:
        st.session_state.current_query = ""
    if 'current_result' not in st.session_state:
        st.session_state.current_result = None
    if 'current_metadata' not in st.session_state:
        st.session_state.current_metadata = None
    if 'app_initialized' not in st.session_state:
        st.session_state.app_initialized = False
    if 'connection_status' not in st.session_state:
        st.session_state.connection_status = {}

def initialize_app():
    """初始化应用"""
    if st.session_state.app_initialized:
        return True

    with st.spinner("正在初始化应用..."):
        try:
            # 创建SQL生成器
            st.session_state.sql_generator = SQLGenerator()

            # 测试连接
            st.session_state.connection_status = st.session_state.sql_generator.test_connection()

            # 检查连接状态
            all_connected = all(st.session_state.connection_status.values())

            if all_connected:
                st.session_state.app_initialized = True
                return True
            else:
                # 显示连接状态
                st.warning("部分连接测试失败，应用功能可能受限:")

                for service, status in st.session_state.connection_status.items():
                    if status:
                        st.success(f"✓ {service}: 连接成功")
                    else:
                        st.error(f"✗ {service}: 连接失败")

                # 即使部分连接失败，也允许继续使用
                st.session_state.app_initialized = True
                st.info("应用已初始化，部分功能可能不可用")
                return True

        except Exception as e:
            st.error(f"应用初始化失败: {e}")
            st.info("请检查配置和网络连接")
            return False

def render_sidebar():
    """渲染侧边栏"""
    with st.sidebar:
        st.title("🔧 控制面板")

        # 配置信息
        with st.expander("📋 配置信息", expanded=False):
            config_summary = get_config_summary()
            for key, value in config_summary.items():
                st.text(f"{key}: {value}")

        # 连接状态
        with st.expander("🔌 连接状态", expanded=False):
            if st.session_state.connection_status:
                for service, status in st.session_state.connection_status.items():
                    if status:
                        st.success(f"✓ {service}")
                    else:
                        st.error(f"✗ {service}")
            else:
                st.info("未测试连接")

        # 统计信息
        with st.expander("📈 使用统计", expanded=False):
            if st.session_state.sql_generator:
                stats = st.session_state.sql_generator.get_stats()
                for key, value in stats.items():
                    st.text(f"{key}: {value}")

        # 历史记录
        with st.expander("📚 查询历史", expanded=False):
            recent_queries = st.session_state.query_history.get_recent_queries(5)
            if recent_queries:
                for query in recent_queries:
                    with st.container():
                        st.caption(f"{query['timestamp'][:16]}")
                        st.text(f"{query['natural_language'][:50]}...")
                        if st.button(f"使用", key=f"use_{query['id']}"):
                            st.session_state.current_query = query['natural_language']
                            st.rerun()
            else:
                st.info("暂无查询历史")

        # 示例查询
        with st.expander("💡 示例查询", expanded=True):
            examples = create_example_queries()
            for example in examples:
                if st.button(f"{example['title']}", key=f"example_{example['title']}"):
                    st.session_state.current_query = example['query']
                    st.rerun()
                st.caption(example['description'])

        # 操作按钮
        st.divider()
        col1, col2 = st.columns(2)
        with col1:
            if st.button("🔄 重新初始化", use_container_width=True):
                st.session_state.app_initialized = False
                st.rerun()
        with col2:
            if st.button("🗑️ 清空历史", use_container_width=True):
                st.session_state.query_history.clear_history()
                st.rerun()

def render_main_content():
    """渲染主内容区域"""
    # 标题
    st.title("📊 审计凭证自然语言查询工具")
    st.markdown("使用自然语言查询审计凭证数据库，无需编写SQL语句")

    # 三列布局
    col1, col2, col3 = st.columns([1, 1, 2])

    # 第一列：自然语言输入
    with col1:
        st.header("📝 自然语言输入")

        # 查询输入框
        query_text = st.text_area(
            "输入您的查询需求：",
            value=st.session_state.current_query,
            height=200,
            placeholder="例如：查询2024年和立公司的管理费用",
            key="query_input"
        )

        # 查询按钮
        col_btn1, col_btn2 = st.columns(2)
        with col_btn1:
            if st.button("🚀 生成并执行", type="primary", use_container_width=True):
                if query_text:
                    execute_query(query_text)
                else:
                    st.warning("请输入查询内容")
        with col_btn2:
            if st.button("🔄 仅生成SQL", use_container_width=True):
                if query_text:
                    generate_sql_only(query_text)
                else:
                    st.warning("请输入查询内容")

        # 输入验证
        if query_text:
            is_valid, error_msg = validate_natural_language_query(query_text)
            if not is_valid:
                st.warning(error_msg)

    # 第二列：生成的SQL
    with col2:
        st.header("🔍 生成的SQL")

        if st.session_state.current_metadata and 'final_sql' in st.session_state.current_metadata:
            sql = st.session_state.current_metadata['final_sql']
            st.code(format_sql_for_display(sql), language="sql")

            # SQL操作按钮
            col_sql1, col_sql2 = st.columns(2)
            with col_sql1:
                if st.button("📋 复制SQL", use_container_width=True):
                    st.code(sql, language="sql")
                    st.success("SQL已复制到剪贴板")
            with col_sql2:
                if st.button("▶️ 重新执行", use_container_width=True):
                    if sql:
                        execute_sql_directly(sql)
        else:
            st.info("生成的SQL将显示在这里")

        # 显示元数据
        if st.session_state.current_metadata:
            with st.expander("📊 执行详情", expanded=False):
                metadata = st.session_state.current_metadata
                if 'steps' in metadata:
                    for step in metadata['steps']:
                        st.text(f"{step['step']}: {step.get('time', 0):.2f}秒")
                if 'total_time' in metadata:
                    st.text(f"总耗时: {metadata['total_time']:.2f}秒")
                if 'result_shape' in metadata:
                    shape = metadata['result_shape']
                    st.text(f"结果形状: {shape['rows']}行 × {shape['columns']}列")

    # 第三列：查询结果
    with col3:
        st.header("📋 查询结果")

        if st.session_state.current_result is not None:
            result_df = st.session_state.current_result

            # 显示数据表格
            st.dataframe(
                result_df,
                use_container_width=True,
                hide_index=True
            )

            # 结果统计
            col_res1, col_res2, col_res3 = st.columns(3)
            with col_res1:
                st.metric("行数", len(result_df))
            with col_res2:
                st.metric("列数", len(result_df.columns))
            with col_res3:
                if st.session_state.current_metadata and 'total_time' in st.session_state.current_metadata:
                    st.metric("耗时", f"{st.session_state.current_metadata['total_time']:.2f}秒")

            # 结果操作按钮
            col_act1, col_act2, col_act3 = st.columns(3)
            with col_act1:
                if st.button("💾 保存结果", use_container_width=True):
                    save_result(result_df)
            with col_act2:
                if st.button("📈 可视化", use_container_width=True):
                    show_visualization(result_df)
            with col_act3:
                if st.button("📄 导出CSV", use_container_width=True):
                    export_to_csv(result_df)

        else:
            st.info("查询结果将显示在这里")

def execute_query(natural_language: str):
    """执行自然语言查询"""
    with st.spinner("正在生成SQL语句..."):
        try:
            # 生成SQL并执行查询
            result, metadata = st.session_state.sql_generator.nl_to_result(natural_language)

            # 更新会话状态
            st.session_state.current_result = result
            st.session_state.current_metadata = metadata
            st.session_state.current_query = natural_language

            # 添加到历史记录
            st.session_state.query_history.add_query(
                natural_language=natural_language,
                sql=metadata.get('final_sql', ''),
                result_metadata=metadata
            )

            st.success("查询执行成功！")

        except SQLGenerationError as e:
            st.error(f"查询执行失败: {e}")
            st.session_state.current_result = None
            st.session_state.current_metadata = {
                "error": str(e),
                "success": False
            }

        except Exception as e:
            st.error(f"发生未知错误: {e}")
            st.session_state.current_result = None
            st.session_state.current_metadata = {
                "error": str(e),
                "success": False
            }

def generate_sql_only(natural_language: str):
    """仅生成SQL，不执行"""
    with st.spinner("正在生成SQL语句..."):
        try:
            sql, metadata = st.session_state.sql_generator.nl_to_sql(natural_language)

            # 更新会话状态
            st.session_state.current_metadata = {
                "final_sql": sql,
                "generation_time": metadata.get('generation_time', 0),
                "success": True,
                "steps": [{
                    "step": "sql_generation",
                    "time": metadata.get('generation_time', 0),
                    "sql": sql,
                    **metadata
                }]
            }
            st.session_state.current_query = natural_language

            st.success("SQL生成成功！")

        except SQLGenerationError as e:
            st.error(f"SQL生成失败: {e}")
            st.session_state.current_metadata = {
                "error": str(e),
                "success": False
            }

def execute_sql_directly(sql: str):
    """直接执行SQL语句"""
    with st.spinner("正在执行查询..."):
        try:
            result, metadata = st.session_state.sql_generator.execute_query(sql)

            # 更新会话状态
            st.session_state.current_result = result
            if st.session_state.current_metadata:
                st.session_state.current_metadata.update({
                    "execution_metadata": metadata,
                    "result_shape": {
                        "rows": len(result),
                        "columns": len(result.columns) if hasattr(result, 'columns') else 0
                    }
                })

            st.success("SQL执行成功！")

        except SQLGenerationError as e:
            st.error(f"SQL执行失败: {e}")

def save_result(result_df: pd.DataFrame):
    """保存查询结果"""
    try:
        from utils import save_query_result
        filepath = save_query_result(
            result_df,
            st.session_state.current_metadata,
            "query_results"
        )
        st.success(f"结果已保存到: {filepath}")
    except Exception as e:
        st.error(f"保存失败: {e}")

def show_visualization(result_df: pd.DataFrame):
    """显示数据可视化"""
    try:
        # 简单的可视化示例
        st.subheader("数据可视化")

        # 数值列的可视化
        numeric_cols = result_df.select_dtypes(include=['number']).columns.tolist()
        if numeric_cols:
            selected_col = st.selectbox("选择数值列", numeric_cols)
            if selected_col:
                st.bar_chart(result_df[selected_col].head(20))
        else:
            st.info("没有数值列可用于可视化")

        # 数据分布
        if len(result_df) > 0:
            st.subheader("数据摘要")
            st.write(result_df.describe())

    except Exception as e:
        st.error(f"可视化失败: {e}")

def export_to_csv(result_df: pd.DataFrame):
    """导出为CSV"""
    try:
        csv = result_df.to_csv(index=False).encode('utf-8-sig')
        st.download_button(
            label="📥 下载CSV",
            data=csv,
            file_name=f"query_result_{time.strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
            use_container_width=True
        )
    except Exception as e:
        st.error(f"导出失败: {e}")

def main():
    """主函数"""
    # 初始化
    init_session_state()

    # 应用初始化
    if not st.session_state.app_initialized:
        if initialize_app():
            st.rerun()
        else:
            st.error("应用初始化失败，请检查配置和连接")
            return

    # 渲染界面
    render_sidebar()
    render_main_content()

    # 页脚
    st.divider()
    st.caption("📊 审计凭证自然语言查询工具 | 基于DeepSeek API和Streamlit")

if __name__ == "__main__":
    main()