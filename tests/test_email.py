"""Tests for tez.services.email module."""

from unittest.mock import MagicMock

from tez_server.services.email import EmailService, build_html, build_plain_text


class TestBuildPlainText:
    def test_includes_sharer_name(self) -> None:
        result = build_plain_text(
            sharer_name="Alice",
            tez_name="Notes",
            tez_id="abc-123",
            message=None,
        )
        assert "Alice shared a Tez with you." in result

    def test_includes_tez_details(self) -> None:
        result = build_plain_text(
            sharer_name="Alice",
            tez_name="Sprint Retro",
            tez_id="xyz-999",
            message=None,
        )
        assert "Name: Sprint Retro" in result
        assert "ID:   xyz-999" in result

    def test_includes_download_command(self) -> None:
        result = build_plain_text(
            sharer_name="Alice",
            tez_name="Notes",
            tez_id="abc-123",
            message=None,
        )
        assert "tez download abc-123" in result

    def test_includes_tez_url(self) -> None:
        result = build_plain_text(
            sharer_name="Alice",
            tez_name="Notes",
            tez_id="abc-123",
            message=None,
        )
        assert "https://tez.it/abc-123" in result

    def test_includes_message_when_provided(self) -> None:
        result = build_plain_text(
            sharer_name="Alice",
            tez_name="Notes",
            tez_id="abc-123",
            message="Check this out",
        )
        assert '"Check this out"' in result

    def test_excludes_message_when_none(self) -> None:
        result = build_plain_text(
            sharer_name="Alice",
            tez_name="Notes",
            tez_id="abc-123",
            message=None,
        )
        assert '"' not in result.split("---")[0]


class TestBuildHtml:
    def test_includes_sharer_name(self) -> None:
        result = build_html(
            sharer_name="Bob",
            tez_name="Notes",
            tez_id="abc-123",
            message=None,
        )
        assert "Bob" in result
        assert "shared a Tez with you" in result

    def test_includes_tez_name_and_id(self) -> None:
        result = build_html(
            sharer_name="Bob",
            tez_name="RCA: Login Bug",
            tez_id="def-456",
            message=None,
        )
        assert "RCA: Login Bug" in result
        assert "def-456" in result

    def test_includes_tez_url(self) -> None:
        result = build_html(
            sharer_name="Bob",
            tez_name="Notes",
            tez_id="abc-123",
            message=None,
        )
        assert "https://tez.it/abc-123" in result

    def test_includes_download_command(self) -> None:
        result = build_html(
            sharer_name="Bob",
            tez_name="Notes",
            tez_id="abc-123",
            message=None,
        )
        assert "tez download abc-123" in result

    def test_includes_message_when_provided(self) -> None:
        result = build_html(
            sharer_name="Bob",
            tez_name="Notes",
            tez_id="abc-123",
            message="Take a look",
        )
        assert "Take a look" in result

    def test_excludes_message_block_when_none(self) -> None:
        result = build_html(
            sharer_name="Bob",
            tez_name="Notes",
            tez_id="abc-123",
            message=None,
        )
        assert "border-left: 3px solid" not in result

    def test_is_valid_html(self) -> None:
        result = build_html(
            sharer_name="Bob",
            tez_name="Notes",
            tez_id="abc-123",
            message=None,
        )
        assert result.startswith("<!DOCTYPE html>")
        assert "</html>" in result


class TestEmailService:
    def test_send_share_notification_calls_client(self) -> None:
        mock_client = MagicMock()
        mock_client.send.return_value = MagicMock(status_code=202)

        svc = EmailService(client=mock_client)
        status = svc.send_share_notification(
            recipient_email="noor@ragu.ai",
            sharer_name="Adam",
            tez_name="Q1 Notes",
            tez_id="abc-123",
        )

        assert status == 202
        mock_client.send.assert_called_once()

    def test_send_share_notification_builds_correct_mail(self) -> None:
        mock_client = MagicMock()
        mock_client.send.return_value = MagicMock(status_code=202)

        svc = EmailService(client=mock_client, from_email="test@example.com")
        svc.send_share_notification(
            recipient_email="noor@ragu.ai",
            sharer_name="Adam",
            tez_name="Q1 Notes",
            tez_id="abc-123",
            message="Hey!",
        )

        mail = mock_client.send.call_args[0][0]
        assert mail.subject.subject == "Adam shared a Tez with you"
        assert mail.from_email.email == "test@example.com"

    def test_send_share_notification_with_message(self) -> None:
        mock_client = MagicMock()
        mock_client.send.return_value = MagicMock(status_code=202)

        svc = EmailService(client=mock_client)
        status = svc.send_share_notification(
            recipient_email="noor@ragu.ai",
            sharer_name="Adam",
            tez_name="Q1 Notes",
            tez_id="abc-123",
            message="Check this",
        )

        assert status == 202

    def test_from_api_key_factory(self) -> None:
        svc = EmailService.from_api_key("fake-key")
        assert svc._from_email == "noreply@tezit.com"
