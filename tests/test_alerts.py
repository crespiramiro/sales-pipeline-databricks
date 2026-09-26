import sys
from unittest.mock import patch, MagicMock
from pathlib import Path

# Add project root and ingestion to sys.path
sys.path.insert(0, str(Path(__file__).parent.parent / "ingestion"))

from utils.alerts import send_alert, send_alert_simple


def test_send_alert_missing_key(monkeypatch):
    monkeypatch.setattr("utils.alerts.RESEND_API_KEY", None)
    result = send_alert("test@example.com", "Subject", "Body")
    assert result is False


@patch("requests.post")
def test_send_alert_success(mock_post, monkeypatch):
    monkeypatch.setattr("utils.alerts.RESEND_API_KEY", "re_test_123")
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_post.return_value = mock_response

    result = send_alert_simple("test@example.com", "Subject", "Body")
    assert result is True
    assert mock_post.called
