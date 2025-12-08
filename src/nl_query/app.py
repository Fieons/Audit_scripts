"""
Streamlit主应用
自然语言SQL查询工具的Web界面
"""

import sys
import os

# 添加项目根目录到Python路径，确保相对导入能工作
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import streamlit as st
import pandas as pd
import time
from typing import Optional, Dict, Any
import logging

# 使用绝对导入
try:
    from src.nl_query.config import validate_config, get_config_summary
    from src.nl_query.sql_generator import SQLGenerator, SQLGenerationError
    from src.nl_query.clients.chat import ChatClient
    from src.nl_query.chat_context import ChatContext
    from src.nl_query.utils import (
        setup_logging, format_error_message, format_sql_for_display,
        format_dataframe_for_display, validate_natural_language_query
    )
except ImportError:
    # 如果绝对导入失败，尝试相对导入（用于直接运行的情况）
    from .config import validate_config, get_config_summary
    from .sql_generator import SQLGenerator, SQLGenerationError
    from .clients.chat import ChatClient
    from .chat_context import ChatContext
    from .utils import (
        setup_logging, format_error_message, format_sql_for_display,
        format_dataframe_for_display, validate_natural_language_query
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
    # 查询相关状态
    if 'sql_generator' not in st.session_state:
        st.session_state.sql_generator = None
    if 'current_query' not in st.session_state:
        st.session_state.current_query = ""
    if 'current_result' not in st.session_state:
        st.session_state.current_result = None
    if 'current_metadata' not in st.session_state:
        st.session_state.current_metadata = None
    if 'editable_sql' not in st.session_state:
        st.session_state.editable_sql = ""

    # 聊天相关状态
    if 'chat_client' not in st.session_state:
        st.session_state.chat_client = None
    if 'chat_context' not in st.session_state:
        st.session_state.chat_context = None
    if 'chat_history' not in st.session_state:
        st.session_state.chat_history = []
    if 'current_chat_message' not in st.session_state:
        st.session_state.current_chat_message = ""

    # 应用状态
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

            # 创建聊天客户端和上下文
            st.session_state.chat_client = ChatClient()
            st.session_state.chat_context = ChatContext()

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


def render_main_content():
    """渲染主内容区域 - 新的2:1布局"""
    # 标题
    st.title("📊 审计凭证自然语言查询工具")
    st.markdown("使用自然语言查询审计凭证数据库，无需编写SQL语句")

    # 2:1布局 - 左侧查询功能，右侧聊天
    col_left, col_right = st.columns([2, 1])

    # 左侧：查询功能
    with col_left:
        render_query_section()

    # 右侧：聊天功能
    with col_right:
        render_chat_section()

def render_query_section():
    """渲染查询功能区域（左侧）"""
    # 左侧分为上下两部分
    col_left_top, col_left_bottom = st.columns([1, 1])

    # 左上：自然语言输入和SQL生成
    with col_left_top:
        render_query_input_section()

    # 左下：查询结果
    with col_left_bottom:
        render_query_result_section()

def render_query_input_section():
    """渲染查询输入区域"""
    st.header("📝 查询输入")

    # 查询输入框
    query_text = st.text_area(
        "输入您的查询需求：",
        value=st.session_state.current_query,
        height=150,
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

    # 生成的SQL显示和编辑
    st.header("🔍 生成的SQL")
    if st.session_state.current_metadata and 'final_sql' in st.session_state.current_metadata:
        # 在会话状态中存储可编辑的SQL
        if 'editable_sql' not in st.session_state:
            st.session_state.editable_sql = st.session_state.current_metadata['final_sql']

        # SQL编辑区域
        editable_sql = st.text_area(
            "编辑SQL语句：",
            value=st.session_state.editable_sql,
            height=150,
            key="sql_editor"
        )

        # 更新可编辑的SQL
        if editable_sql != st.session_state.editable_sql:
            st.session_state.editable_sql = editable_sql

        # 格式化显示
        st.caption("格式化显示：")
        st.code(format_sql_for_display(editable_sql), language="sql")

        # SQL操作按钮 - 使用3列布局
        col_sql1, col_sql2, col_sql3 = st.columns(3)
        with col_sql1:
            if st.button("📋 复制SQL", key="copy_sql_btn", use_container_width=True):
                st.code(editable_sql, language="sql")
                st.success("SQL已复制到剪贴板")
        with col_sql2:
            if st.button("▶️ 执行编辑后的SQL", key="execute_edited_btn", type="primary", use_container_width=True):
                if editable_sql:
                    execute_sql_directly(editable_sql)
        with col_sql3:
            if st.button("🔄 恢复原SQL", key="restore_sql_btn", use_container_width=True):
                st.session_state.editable_sql = st.session_state.current_metadata['final_sql']
                st.rerun()

        # 更新聊天上下文按钮
        if st.button("💬 讨论此查询", key="discuss_query_btn", use_container_width=True):
            # 使用当前显示的SQL（可能是编辑后的）更新上下文
            update_chat_context_from_query_with_sql(editable_sql)
    else:
        st.info("生成的SQL将显示在这里")

def render_query_result_section():
    """渲染查询结果区域"""
    st.header("📋 查询结果")

    if st.session_state.current_result is not None:
        result_df = st.session_state.current_result

        # 显示数据表格
        st.dataframe(
            result_df,
            use_container_width=True,
            hide_index=True,
            height=300
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
            if st.button("💾 保存结果", key="save_result_btn", use_container_width=True):
                save_result(result_df)
        with col_act2:
            if st.button("📈 可视化", key="visualize_btn", use_container_width=True):
                show_visualization(result_df)
        with col_act3:
            if st.button("📄 导出CSV", key="export_csv_btn", use_container_width=True):
                export_to_csv(result_df)

        # 执行详情
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

    else:
        st.info("查询结果将显示在这里")

def render_chat_section():
    """渲染聊天区域（右侧）"""
    st.header("💬 查询讨论")

    # 显示当前查询上下文状态
    if st.session_state.chat_context and st.session_state.chat_context.generated_sql:
        with st.expander("📋 当前讨论的查询", expanded=False):
            st.caption("原始查询需求")
            st.info(st.session_state.chat_context.natural_language_query)

            st.caption("生成的SQL")
            st.code(st.session_state.chat_context.generated_sql[:200] + "..." if len(st.session_state.chat_context.generated_sql) > 200 else st.session_state.chat_context.generated_sql, language="sql")

            if st.session_state.chat_context.query_result_summary:
                st.caption("查询结果摘要")
                st.info(st.session_state.chat_context.query_result_summary)
    else:
        st.info("💡 提示：执行查询后，点击'讨论此查询'按钮，将当前查询加载到聊天上下文。")

    # 聊天历史显示
    chat_container = st.container(height=350)
    with chat_container:
        display_chat_history()

    # 聊天输入和操作
    render_chat_input_section()

def display_chat_history():
    """显示聊天历史"""
    if not st.session_state.chat_history:
        st.info("暂无聊天记录。执行查询后可以点击'讨论此查询'开始讨论。")
        return

    for message in st.session_state.chat_history:
        if message["role"] == "user":
            with st.chat_message("user"):
                st.write(message["content"])
        else:  # assistant
            with st.chat_message("assistant"):
                # 支持Markdown渲染
                st.markdown(message["content"])

def render_chat_input_section():
    """渲染聊天输入区域"""
    # 聊天操作按钮
    col_chat1, col_chat2 = st.columns(2)
    with col_chat1:
        if st.button("🗑️ 清空聊天", key="clear_chat_btn", use_container_width=True):
            clear_chat_history()
    with col_chat2:
        if st.button("📋 导出对话", key="export_chat_btn", use_container_width=True):
            export_chat_history()

    # 聊天输入
    chat_input = st.chat_input("输入您的讨论内容...", key="chat_input")
    if chat_input:
        process_chat_message(chat_input)

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

            # 更新可编辑的SQL
            if 'final_sql' in metadata:
                st.session_state.editable_sql = metadata['final_sql']
            elif 'steps' in metadata:
                # 从步骤中查找SQL
                for step in metadata['steps']:
                    if 'sql' in step:
                        st.session_state.editable_sql = step['sql']
                        break

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
            st.session_state.editable_sql = sql

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
        from .utils import save_query_result
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

# ==================== 聊天相关函数 ====================

def update_chat_context_from_query():
    """从当前查询更新聊天上下文"""
    import logging
    logger = logging.getLogger(__name__)

    if not st.session_state.current_metadata:
        st.warning("没有可用的查询信息")
        return

    try:
        # 调试：检查当前元数据
        logger.info(f"更新聊天上下文 - 当前元数据: {st.session_state.current_metadata.keys()}")

        # 尝试从不同位置获取SQL
        sql = ""
        if 'final_sql' in st.session_state.current_metadata:
            sql = st.session_state.current_metadata['final_sql']
            logger.info(f"从final_sql获取SQL: {sql[:100]}...")
        elif 'steps' in st.session_state.current_metadata:
            # 从步骤中查找SQL
            for step in st.session_state.current_metadata['steps']:
                if 'sql' in step:
                    sql = step['sql']
                    logger.info(f"从步骤中获取SQL: {sql[:100]}...")
                    break

        if not sql:
            logger.warning("未找到SQL语句")
            st.warning("未找到生成的SQL语句")
            return

        # 获取当前查询的上下文信息
        context_info = st.session_state.sql_generator.get_current_context(
            natural_language=st.session_state.current_query,
            sql=sql,
            result=st.session_state.current_result,
            metadata=st.session_state.current_metadata
        )

        # 调试信息
        logger.info(f"更新聊天上下文 - SQL长度: {len(context_info.get('generated_sql', ''))}")
        logger.info(f"更新聊天上下文 - SQL预览: {context_info.get('generated_sql', '')[:100]}...")
        logger.info(f"更新聊天上下文 - 查询需求: {context_info.get('natural_language_query', '')}")

        # 更新聊天上下文
        st.session_state.chat_context.update_query_context(
            natural_language=context_info["natural_language_query"],
            sql=context_info["generated_sql"],
            result_summary=context_info["query_result_summary"],
            execution_time=context_info["query_execution_time"],
            result_shape=context_info["query_result_shape"]
        )

        # 获取并显示上下文摘要（调试用）
        context_summary = st.session_state.chat_context.get_context_summary()
        logger.info(f"聊天上下文摘要: {context_summary[:200]}...")

        # 清空聊天历史，开始新的讨论
        st.session_state.chat_client.clear_history()
        st.session_state.chat_history = []

        st.success("聊天上下文已更新！现在可以开始讨论此查询。")
        st.rerun()

    except Exception as e:
        st.error(f"更新聊天上下文失败: {e}")
        logger.error(f"更新聊天上下文失败: {e}", exc_info=True)

def update_chat_context_from_query_with_sql(custom_sql: str):
    """使用自定义SQL更新聊天上下文"""
    import logging
    logger = logging.getLogger(__name__)

    if not st.session_state.current_metadata:
        st.warning("没有可用的查询信息")
        return

    try:
        # 调试信息
        logger.info(f"使用自定义SQL更新聊天上下文 - SQL长度: {len(custom_sql)}")
        logger.info(f"自定义SQL预览: {custom_sql[:100]}...")

        # 获取当前查询的上下文信息（使用自定义SQL）
        context_info = st.session_state.sql_generator.get_current_context(
            natural_language=st.session_state.current_query,
            sql=custom_sql,
            result=st.session_state.current_result,
            metadata=st.session_state.current_metadata
        )

        # 更新聊天上下文
        st.session_state.chat_context.update_query_context(
            natural_language=context_info["natural_language_query"],
            sql=context_info["generated_sql"],
            result_summary=context_info["query_result_summary"],
            execution_time=context_info["query_execution_time"],
            result_shape=context_info["query_result_shape"]
        )

        # 获取并显示上下文摘要（调试用）
        context_summary = st.session_state.chat_context.get_context_summary()
        logger.info(f"聊天上下文摘要: {context_summary[:200]}...")

        # 清空聊天历史，开始新的讨论
        st.session_state.chat_client.clear_history()
        st.session_state.chat_history = []

        st.success("聊天上下文已更新（使用编辑后的SQL）！现在可以开始讨论此查询。")
        st.rerun()

    except Exception as e:
        st.error(f"更新聊天上下文失败: {e}")
        logger.error(f"更新聊天上下文失败: {e}", exc_info=True)

def process_chat_message(user_message: str):
    """处理聊天消息"""
    if not st.session_state.chat_client:
        st.error("聊天客户端未初始化")
        return

    # 调试信息
    import logging
    logger = logging.getLogger(__name__)
    logger.info(f"处理聊天消息: {user_message[:100]}...")

    # 检查聊天上下文状态
    if st.session_state.chat_context:
        context_summary = st.session_state.chat_context.get_context_summary()
        logger.info(f"当前聊天上下文摘要: {context_summary[:200]}...")
        logger.info(f"聊天上下文SQL: {st.session_state.chat_context.generated_sql[:100] if st.session_state.chat_context.generated_sql else '空'}")
    else:
        logger.warning("聊天上下文未初始化")

    # 添加用户消息到聊天历史
    st.session_state.chat_history.append({
        "role": "user",
        "content": user_message,
        "timestamp": time.time()
    })

    # 更新聊天上下文中的讨论历史
    st.session_state.chat_context.add_discussion_message("user", user_message)

    # 获取上下文信息
    context_info = st.session_state.chat_context.get_context_summary()
    logger.info(f"发送给AI的上下文信息长度: {len(context_info)}")
    logger.info(f"发送给AI的上下文信息预览: {context_info[:200]}...")

    # 发送消息并获取AI回复
    with st.spinner("正在思考..."):
        try:
            assistant_message = st.session_state.chat_client.send_message(
                user_message=user_message,
                context_info=context_info
            )

            # 添加AI回复到聊天历史
            st.session_state.chat_history.append({
                "role": "assistant",
                "content": assistant_message,
                "timestamp": time.time()
            })

            # 更新聊天上下文中的讨论历史
            st.session_state.chat_context.add_discussion_message("assistant", assistant_message)

            # 刷新界面显示新消息
            st.rerun()

        except Exception as e:
            error_msg = f"聊天处理失败: {e}"
            st.error(error_msg)

            # 添加错误消息到历史
            st.session_state.chat_history.append({
                "role": "assistant",
                "content": f"抱歉，处理消息时出现错误: {str(e)}",
                "timestamp": time.time(),
                "error": True
            })

def clear_chat_history():
    """清空聊天历史"""
    st.session_state.chat_client.clear_history()
    st.session_state.chat_history = []
    st.session_state.chat_context.reset()
    st.success("聊天历史已清空")
    st.rerun()

def export_chat_history():
    """导出聊天历史"""
    if not st.session_state.chat_history:
        st.warning("没有聊天记录可导出")
        return

    try:
        # 构建Markdown格式的聊天记录
        markdown_content = "# 查询讨论记录\n\n"
        markdown_content += f"生成时间: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n"

        # 添加上下文信息
        if st.session_state.chat_context:
            context_summary = st.session_state.chat_context.get_context_summary()
            markdown_content += "## 查询上下文\n\n"
            markdown_content += f"{context_summary}\n\n"

        # 添加聊天记录
        markdown_content += "## 对话记录\n\n"
        for msg in st.session_state.chat_history:
            role = "用户" if msg["role"] == "user" else "助手"
            timestamp = time.strftime("%H:%M:%S", time.localtime(msg.get("timestamp", time.time())))
            content = msg["content"]

            markdown_content += f"### {role} ({timestamp})\n\n"
            markdown_content += f"{content}\n\n"

        # 提供下载
        st.download_button(
            label="📥 下载对话记录",
            data=markdown_content.encode('utf-8'),
            file_name=f"chat_history_{time.strftime('%Y%m%d_%H%M%S')}.md",
            mime="text/markdown",
            use_container_width=True
        )

    except Exception as e:
        st.error(f"导出聊天记录失败: {e}")

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