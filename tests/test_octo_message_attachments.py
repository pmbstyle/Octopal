from __future__ import annotations

from octopal.runtime.octo.message_runtime import _build_user_memory_content


def test_user_memory_content_preserves_image_only_attachment_path() -> None:
    content = _build_user_memory_content(
        "",
        images=["data:image/jpeg;base64,AAA"],
        saved_file_paths=["/workspace/tmp/telegram_images/img_test.jpg"],
    )

    assert "User uploaded image attachment(s)." in content
    assert "image_count=1" in content
    assert "/workspace/tmp/telegram_images/img_test.jpg" in content


def test_user_memory_content_adds_attachment_paths_to_text_turn() -> None:
    content = _build_user_memory_content(
        "what is on this screenshot?",
        images=["data:image/png;base64,AAA"],
        saved_file_paths=["/workspace/tmp/telegram_images/screen.png"],
    )

    assert content.startswith("what is on this screenshot?")
    assert "saved_file_paths" in content
    assert "/workspace/tmp/telegram_images/screen.png" in content
