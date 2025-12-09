"""
SQS Consumer module for reading messages from the analytics queue.

This module handles:
- Connection to AWS SQS
- Message polling with long polling
- Message deletion after successful processing
- Error handling and retry logic
"""
import json
import logging
from typing import Optional
import boto3
from botocore.exceptions import ClientError

import config

logger = logging.getLogger(__name__)


class SQSConsumer:
    """Consumer class for reading messages from AWS SQS."""

    def __init__(
        self,
        queue_url: Optional[str] = None,
        region: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
    ):
        """
        Initialize the SQS consumer.

        Args:
            queue_url: The URL of the SQS queue. If not provided, will be resolved from queue name.
            region: AWS region. Defaults to config value.
            aws_access_key_id: AWS access key. Defaults to config value.
            aws_secret_access_key: AWS secret key. Defaults to config value.
            aws_session_token: AWS session token. Defaults to config value.
        """
        self.region = region or config.AWS_REGION
        self.aws_access_key_id = aws_access_key_id or config.AWS_ACCESS_KEY_ID
        self.aws_secret_access_key = aws_secret_access_key or config.AWS_SECRET_ACCESS_KEY
        self.aws_session_token = aws_session_token or config.AWS_SESSION_TOKEN

        # Initialize SQS client
        self.sqs_client = boto3.client(
            "sqs",
            region_name=self.region,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            aws_session_token=self.aws_session_token,
        )

        # Resolve queue URL
        self.queue_url = queue_url or config.SQS_QUEUE_URL
        if not self.queue_url:
            self.queue_url = self._get_queue_url(config.SQS_QUEUE_NAME)

        logger.info(f"SQS Consumer initialized for queue: {self.queue_url}")

    def _get_queue_url(self, queue_name: str) -> str:
        """
        Get the queue URL from the queue name.

        Args:
            queue_name: The name of the SQS queue.

        Returns:
            The URL of the queue.

        Raises:
            ClientError: If the queue is not found or access is denied.
        """
        try:
            response = self.sqs_client.get_queue_url(QueueName=queue_name)
            return response["QueueUrl"]
        except ClientError as e:
            logger.error(f"Failed to get queue URL for {queue_name}: {e}")
            raise

    def receive_messages(
        self,
        max_messages: int = None,
        visibility_timeout: int = None,
        wait_time_seconds: int = None,
    ) -> list[dict]:
        """
        Receive messages from the SQS queue.

        Args:
            max_messages: Maximum number of messages to receive (1-10).
            visibility_timeout: How long the message is hidden from other consumers.
            wait_time_seconds: How long to wait for messages (long polling).

        Returns:
            List of messages with their metadata.
        """
        max_messages = max_messages or config.MAX_MESSAGES_PER_BATCH
        visibility_timeout = visibility_timeout or config.VISIBILITY_TIMEOUT
        wait_time_seconds = wait_time_seconds or config.WAIT_TIME_SECONDS

        try:
            response = self.sqs_client.receive_message(
                QueueUrl=self.queue_url,
                MaxNumberOfMessages=min(max_messages, 10),
                VisibilityTimeout=visibility_timeout,
                WaitTimeSeconds=wait_time_seconds,
                MessageAttributeNames=["All"],
                AttributeNames=["All"],
            )

            messages = response.get("Messages", [])
            logger.debug(f"Received {len(messages)} messages from SQS")

            # Parse message bodies
            parsed_messages = []
            for msg in messages:
                try:
                    parsed_msg = {
                        "message_id": msg["MessageId"],
                        "receipt_handle": msg["ReceiptHandle"],
                        "body": json.loads(msg["Body"]),
                        "attributes": msg.get("Attributes", {}),
                        "message_attributes": msg.get("MessageAttributes", {}),
                    }
                    parsed_messages.append(parsed_msg)
                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to parse message {msg['MessageId']}: {e}")
                    # Still include the message but with raw body
                    parsed_messages.append({
                        "message_id": msg["MessageId"],
                        "receipt_handle": msg["ReceiptHandle"],
                        "body": msg["Body"],
                        "parse_error": str(e),
                    })

            return parsed_messages

        except ClientError as e:
            logger.error(f"Error receiving messages: {e}")
            raise

    def delete_message(self, receipt_handle: str) -> bool:
        """
        Delete a message from the queue after successful processing.

        Args:
            receipt_handle: The receipt handle of the message to delete.

        Returns:
            True if deletion was successful, False otherwise.
        """
        try:
            self.sqs_client.delete_message(
                QueueUrl=self.queue_url,
                ReceiptHandle=receipt_handle,
            )
            logger.debug(f"Deleted message with receipt handle: {receipt_handle[:20]}...")
            return True
        except ClientError as e:
            logger.error(f"Failed to delete message: {e}")
            return False

    def delete_messages_batch(self, receipt_handles: list[str]) -> dict:
        """
        Delete multiple messages from the queue in a single request.

        Args:
            receipt_handles: List of receipt handles to delete.

        Returns:
            Dictionary with 'successful' and 'failed' lists.
        """
        if not receipt_handles:
            return {"successful": [], "failed": []}

        # SQS allows max 10 messages per batch delete
        entries = [
            {"Id": str(i), "ReceiptHandle": handle}
            for i, handle in enumerate(receipt_handles[:10])
        ]

        try:
            response = self.sqs_client.delete_message_batch(
                QueueUrl=self.queue_url,
                Entries=entries,
            )

            successful = response.get("Successful", [])
            failed = response.get("Failed", [])

            if failed:
                logger.warning(f"Failed to delete {len(failed)} messages: {failed}")

            logger.info(f"Batch deleted {len(successful)} messages")
            return {"successful": successful, "failed": failed}

        except ClientError as e:
            logger.error(f"Batch delete failed: {e}")
            raise

    def get_queue_attributes(self) -> dict:
        """
        Get attributes of the queue (message count, etc.).

        Returns:
            Dictionary of queue attributes.
        """
        try:
            response = self.sqs_client.get_queue_attributes(
                QueueUrl=self.queue_url,
                AttributeNames=["All"],
            )
            return response.get("Attributes", {})
        except ClientError as e:
            logger.error(f"Failed to get queue attributes: {e}")
            raise

    def get_approximate_message_count(self) -> int:
        """
        Get the approximate number of messages in the queue.

        Returns:
            Approximate number of visible messages.
        """
        attrs = self.get_queue_attributes()
        return int(attrs.get("ApproximateNumberOfMessages", 0))

