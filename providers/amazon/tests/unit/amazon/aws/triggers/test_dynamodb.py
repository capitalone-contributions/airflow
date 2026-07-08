# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

from unittest import mock
from unittest.mock import AsyncMock, MagicMock

import pytest

from airflow.providers.amazon.aws.triggers.dynamodb import DynamoDBValueTrigger
from airflow.triggers.base import TriggerEvent

TEST_TABLE_NAME = "test_table"
TEST_PK_NAME = "PK"
TEST_PK_VALUE = "pk_val"
TEST_SK_NAME = "SK"
TEST_SK_VALUE = "sk_val"
TEST_ATTRIBUTE_NAME = "status"
TEST_ATTRIBUTE_VALUE = "COMPLETED"
TEST_AWS_CONN_ID = "aws_default"
TEST_REGION_NAME = "us-east-1"
TEST_WAITER_DELAY = 1


class TestDynamoDBValueTrigger:
    def setup_method(self):
        self.trigger = DynamoDBValueTrigger(
            table_name=TEST_TABLE_NAME,
            partition_key_name=TEST_PK_NAME,
            partition_key_value=TEST_PK_VALUE,
            attribute_name=TEST_ATTRIBUTE_NAME,
            attribute_value=TEST_ATTRIBUTE_VALUE,
            waiter_delay=TEST_WAITER_DELAY,
            aws_conn_id=TEST_AWS_CONN_ID,
            region_name=TEST_REGION_NAME,
        )

    def test_serialize(self):
        class_path, kwargs = self.trigger.serialize()
        assert class_path == "airflow.providers.amazon.aws.triggers.dynamodb.DynamoDBValueTrigger"
        assert kwargs["table_name"] == TEST_TABLE_NAME
        assert kwargs["partition_key_name"] == TEST_PK_NAME
        assert kwargs["partition_key_value"] == TEST_PK_VALUE
        assert kwargs["attribute_name"] == TEST_ATTRIBUTE_NAME
        assert kwargs["attribute_value"] == TEST_ATTRIBUTE_VALUE
        assert kwargs["sort_key_name"] is None
        assert kwargs["sort_key_value"] is None
        assert kwargs["waiter_delay"] == TEST_WAITER_DELAY
        assert kwargs["aws_conn_id"] == TEST_AWS_CONN_ID
        assert kwargs["region_name"] == TEST_REGION_NAME

    def test_serialize_with_sort_key(self):
        trigger = DynamoDBValueTrigger(
            table_name=TEST_TABLE_NAME,
            partition_key_name=TEST_PK_NAME,
            partition_key_value=TEST_PK_VALUE,
            attribute_name=TEST_ATTRIBUTE_NAME,
            attribute_value=TEST_ATTRIBUTE_VALUE,
            sort_key_name=TEST_SK_NAME,
            sort_key_value=TEST_SK_VALUE,
            waiter_delay=TEST_WAITER_DELAY,
        )
        _, kwargs = trigger.serialize()
        assert kwargs["sort_key_name"] == TEST_SK_NAME
        assert kwargs["sort_key_value"] == TEST_SK_VALUE

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.amazon.aws.triggers.dynamodb.DynamoDBValueTrigger.hook")
    async def test_run_yields_success_when_attribute_matches(self, mock_hook_prop):
        mock_client = AsyncMock()
        mock_client.get_item.return_value = {"Item": {TEST_ATTRIBUTE_NAME: {"S": TEST_ATTRIBUTE_VALUE}}}
        mock_context_manager = AsyncMock()
        mock_context_manager.__aenter__.return_value = mock_client
        mock_context_manager.__aexit__.return_value = None
        mock_hook = MagicMock()
        mock_hook.get_async_conn.return_value = mock_context_manager
        mock_hook_prop.__get__ = MagicMock(return_value=mock_hook)

        events = []
        async for event in self.trigger.run():
            events.append(event)

        assert len(events) == 1
        assert events[0] == TriggerEvent({"status": "success"})

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.amazon.aws.triggers.dynamodb.DynamoDBValueTrigger.hook")
    @mock.patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_run_retries_until_attribute_matches(self, mock_sleep, mock_hook_prop):
        mock_client = AsyncMock()
        mock_client.get_item.side_effect = [
            {"Item": {TEST_ATTRIBUTE_NAME: {"S": "PENDING"}}},
            {"Item": {TEST_ATTRIBUTE_NAME: {"S": TEST_ATTRIBUTE_VALUE}}},
        ]
        mock_context_manager = AsyncMock()
        mock_context_manager.__aenter__.return_value = mock_client
        mock_context_manager.__aexit__.return_value = None
        mock_hook = MagicMock()
        mock_hook.get_async_conn.return_value = mock_context_manager
        mock_hook_prop.__get__ = MagicMock(return_value=mock_hook)

        events = []
        async for event in self.trigger.run():
            events.append(event)

        assert mock_sleep.call_count == 1
        assert events[0] == TriggerEvent({"status": "success"})

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.amazon.aws.triggers.dynamodb.DynamoDBValueTrigger.hook")
    async def test_run_matches_one_of_multiple_values(self, mock_hook_prop):
        trigger = DynamoDBValueTrigger(
            table_name=TEST_TABLE_NAME,
            partition_key_name=TEST_PK_NAME,
            partition_key_value=TEST_PK_VALUE,
            attribute_name=TEST_ATTRIBUTE_NAME,
            attribute_value=["APPROVED", "COMPLETED"],
            waiter_delay=TEST_WAITER_DELAY,
        )
        mock_client = AsyncMock()
        mock_client.get_item.return_value = {"Item": {TEST_ATTRIBUTE_NAME: {"S": "APPROVED"}}}
        mock_context_manager = AsyncMock()
        mock_context_manager.__aenter__.return_value = mock_client
        mock_context_manager.__aexit__.return_value = None
        mock_hook = MagicMock()
        mock_hook.get_async_conn.return_value = mock_context_manager
        mock_hook_prop.__get__ = MagicMock(return_value=mock_hook)

        events = []
        async for event in trigger.run():
            events.append(event)

        assert events[0] == TriggerEvent({"status": "success"})

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.amazon.aws.triggers.dynamodb.DynamoDBValueTrigger.hook")
    async def test_run_uses_sort_key_in_request(self, mock_hook_prop):
        trigger = DynamoDBValueTrigger(
            table_name=TEST_TABLE_NAME,
            partition_key_name=TEST_PK_NAME,
            partition_key_value=TEST_PK_VALUE,
            attribute_name=TEST_ATTRIBUTE_NAME,
            attribute_value=TEST_ATTRIBUTE_VALUE,
            sort_key_name=TEST_SK_NAME,
            sort_key_value=TEST_SK_VALUE,
            waiter_delay=TEST_WAITER_DELAY,
        )
        mock_client = AsyncMock()
        mock_client.get_item.return_value = {"Item": {TEST_ATTRIBUTE_NAME: {"S": TEST_ATTRIBUTE_VALUE}}}
        mock_context_manager = AsyncMock()
        mock_context_manager.__aenter__.return_value = mock_client
        mock_context_manager.__aexit__.return_value = None
        mock_hook = MagicMock()
        mock_hook.get_async_conn.return_value = mock_context_manager
        mock_hook_prop.__get__ = MagicMock(return_value=mock_hook)

        async for _ in trigger.run():
            pass

        call_kwargs = mock_client.get_item.call_args.kwargs
        assert call_kwargs["Key"][TEST_SK_NAME] == {"S": TEST_SK_VALUE}
