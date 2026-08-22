"""单元测试：_chat_parts_from_line 解析 OpenAI/Qwen 兼容流式行。"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from api.api_server import _chat_parts_from_line


def test_sse_delta_content():
    line = 'data: {"choices":[{"delta":{"content":"abc"}}]}'
    content, reasoning = _chat_parts_from_line(line)
    assert content == "abc"
    assert reasoning == ""


def test_final_message_content_no_data_prefix():
    line = '{"choices":[{"message":{"content":"final"}}]}'
    content, reasoning = _chat_parts_from_line(line)
    assert content == "final"
    assert reasoning == ""


def test_reasoning_only_delta_returns_empty_content():
    line = 'data: {"choices":[{"delta":{"reasoning_content":"think"}}]}'
    content, reasoning = _chat_parts_from_line(line)
    assert content == ""
    assert reasoning == "think"


def test_done_marker_returns_empty():
    assert _chat_parts_from_line("data: [DONE]") == ("", "")
    assert _chat_parts_from_line("[DONE]") == ("", "")
    assert _chat_parts_from_line("") == ("", "")
