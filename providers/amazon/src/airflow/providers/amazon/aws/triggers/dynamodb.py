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

import asyncio
from collections.abc import AsyncIterator
from typing import Any

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.triggers.base import BaseEventTrigger, TriggerEvent
else:
    from airflow.triggers.base import (  # type: ignore
        BaseTrigger as BaseEventTrigger,
        TriggerEvent,
    )


class DynamoDBValueTrigger(BaseEventTrigger):
    """
    Asynchronously poll a DynamoDB item until a given attribute matches the expected value.

    :param table_name: DynamoDB table name
    :param partition_key_name: DynamoDB partition key name
    :param partition_key_value: DynamoDB partition key value
    :param attribute_name: DynamoDB attribute name to check
    :param attribute_value: Expected value (string) or collection of acceptable values
    :param sort_key_name: (optional) DynamoDB sort key name
    :param sort_key_value: (optional) DynamoDB sort key value
    :param waiter_delay: Seconds to wait between polls (default: 60)
    :param aws_conn_id: Airflow AWS connection id
    :param region_name: AWS region name
    :param verify: Whether to verify SSL certificates
    :param botocore_config: botocore config dict
    """

    def __init__(
        self,
        table_name: str,
        partition_key_name: str,
        partition_key_value: str,
        attribute_name: str,
        attribute_value: str | list[str],
        sort_key_name: str | None = None,
        sort_key_value: str | None = None,
        waiter_delay: int = 60,
        aws_conn_id: str | None = "aws_default",
        region_name: str | None = None,
        verify: bool | str | None = None,
        botocore_config: dict | None = None,
    ):
        self.table_name = table_name
        self.partition_key_name = partition_key_name
        self.partition_key_value = partition_key_value
        self.attribute_name = attribute_name
        self.attribute_value = attribute_value
        self.sort_key_name = sort_key_name
        self.sort_key_value = sort_key_value
        self.waiter_delay = waiter_delay
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.verify = verify
        self.botocore_config = botocore_config

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            self.__class__.__module__ + "." + self.__class__.__qualname__,
            {
                "table_name": self.table_name,
                "partition_key_name": self.partition_key_name,
                "partition_key_value": self.partition_key_value,
                "attribute_name": self.attribute_name,
                "attribute_value": self.attribute_value,
                "sort_key_name": self.sort_key_name,
                "sort_key_value": self.sort_key_value,
                "waiter_delay": self.waiter_delay,
                "aws_conn_id": self.aws_conn_id,
                "region_name": self.region_name,
                "verify": self.verify,
                "botocore_config": self.botocore_config,
            },
        )

    @property
    def hook(self) -> AwsBaseHook:
        return AwsBaseHook(
            aws_conn_id=self.aws_conn_id,
            client_type="dynamodb",
            region_name=self.region_name,
            verify=self.verify,
            config=self.botocore_config,
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        acceptable = (
            {self.attribute_value} if isinstance(self.attribute_value, str) else set(self.attribute_value)
        )
        key: dict[str, str] = {self.partition_key_name: {"S": self.partition_key_value}}  # type: ignore[assignment]
        if self.sort_key_name and self.sort_key_value:
            key[self.sort_key_name] = {"S": self.sort_key_value}  # type: ignore[assignment]

        while True:
            async with await self.hook.get_async_conn() as client:
                response = await client.get_item(TableName=self.table_name, Key=key)
            item = response.get("Item", {})
            raw = item.get(self.attribute_name)
            actual: str | None = None
            if raw is not None:
                actual = next(iter(raw.values()), None)
            if actual is not None and actual in acceptable:
                self.log.info(
                    "DynamoDB item %s matched: %s=%s",
                    key,
                    self.attribute_name,
                    actual,
                )
                yield TriggerEvent({"status": "success"})
                return
            self.log.info(
                "DynamoDB item %s: %s=%r not in %s, retrying in %ds",
                key,
                self.attribute_name,
                actual,
                acceptable,
                self.waiter_delay,
            )
            await asyncio.sleep(self.waiter_delay)
