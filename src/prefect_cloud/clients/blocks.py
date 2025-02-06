from __future__ import annotations

from typing import TYPE_CHECKING

from httpx import HTTPStatusError

from prefect_cloud.clients.base import BaseAsyncClient
from prefect_cloud.utilities.exception import ObjectAlreadyExists, ObjectNotFound
from prefect_cloud.utilities.generics import validate_list

if TYPE_CHECKING:
    from uuid import UUID

    from prefect_cloud.schemas.actions import (
        BlockDocumentCreate,
        BlockDocumentUpdate,
    )
    from prefect_cloud.schemas.objects import (
        BlockDocument,
        BlockType,
        BlockSchema,
    )


class BlocksDocumentAsyncClient(BaseAsyncClient):
    async def create_block_document(
        self,
        block_document: "BlockDocument | BlockDocumentCreate",
        include_secrets: bool = True,
    ) -> "BlockDocument":
        """
        Create a block document in the Prefect API. This data is used to configure a
        corresponding Block.

        Args:
            include_secrets (bool): whether to include secret values
                on the stored Block, corresponding to Pydantic's `SecretStr` and
                `SecretBytes` fields. Note Blocks may not work as expected if
                this is set to `False`.
        """
        block_document_data = block_document.model_dump(
            mode="json",
            exclude_unset=True,
            exclude={"id", "block_schema", "block_type"},
            context={"include_secrets": include_secrets},
            serialize_as_any=True,
        )
        try:
            response = await self.request(
                "POST",
                "/block_documents/",
                json=block_document_data,
            )
        except HTTPStatusError as e:
            if e.response.status_code == 409:
                raise ObjectAlreadyExists(http_exc=e) from e
            else:
                raise
        from prefect_cloud.schemas.objects import BlockDocument

        return BlockDocument.model_validate(response.json())

    async def update_block_document(
        self,
        block_document_id: "UUID",
        block_document: "BlockDocumentUpdate",
    ) -> None:
        """
        Update a block document in the Prefect API.
        """
        try:
            await self.request(
                "PATCH",
                "/block_documents/{id}",
                path_params={"id": block_document_id},
                json=block_document.model_dump(
                    mode="json",
                    exclude_unset=True,
                    include={"data", "merge_existing_data", "block_schema_id"},
                ),
            )
        except HTTPStatusError as e:
            if e.response.status_code == 404:
                raise ObjectNotFound(http_exc=e) from e
            else:
                raise

    async def delete_block_document(self, block_document_id: "UUID") -> None:
        """
        Delete a block document.
        """
        try:
            await self.request(
                "DELETE",
                "/block_documents/{id}",
                path_params={"id": block_document_id},
            )
        except HTTPStatusError as e:
            if e.response.status_code == 404:
                raise ObjectNotFound(http_exc=e) from e
            else:
                raise

    async def read_block_document(
        self,
        block_document_id: "UUID",
        include_secrets: bool = True,
    ) -> "BlockDocument":
        """
        Read the block document with the specified ID.

        Args:
            block_document_id: the block document id
            include_secrets (bool): whether to include secret values
                on the Block, corresponding to Pydantic's `SecretStr` and
                `SecretBytes` fields. These fields are automatically obfuscated
                by Pydantic, but users can additionally choose not to receive
                their values from the API. Note that any business logic on the
                Block may not work if this is `False`.

        Raises:
            httpx.RequestError: if the block document was not found for any reason

        Returns:
            A block document or None.
        """
        try:
            response = await self.request(
                "GET",
                "/block_documents/{id}",
                path_params={"id": block_document_id},
                params=dict(include_secrets=include_secrets),
            )
        except HTTPStatusError as e:
            if e.response.status_code == 404:
                raise ObjectNotFound(http_exc=e) from e
            else:
                raise
        from prefect_cloud.schemas.objects import BlockDocument

        return BlockDocument.model_validate(response.json())

    async def read_block_documents(
        self,
        block_schema_type: str | None = None,
        offset: int | None = None,
        limit: int | None = None,
        include_secrets: bool = True,
    ) -> "list[BlockDocument]":
        """
        Read block documents

        Args:
            block_schema_type: an optional block schema type
            offset: an offset
            limit: the number of blocks to return
            include_secrets (bool): whether to include secret values
                on the Block, corresponding to Pydantic's `SecretStr` and
                `SecretBytes` fields. These fields are automatically obfuscated
                by Pydantic, but users can additionally choose not to receive
                their values from the API. Note that any business logic on the
                Block may not work if this is `False`.

        Returns:
            A list of block documents
        """
        response = await self.request(
            "POST",
            "/block_documents/filter",
            json=dict(
                block_schema_type=block_schema_type,
                offset=offset,
                limit=limit,
                include_secrets=include_secrets,
            ),
        )
        from prefect_cloud.schemas.objects import BlockDocument

        return validate_list(BlockDocument, response.json())

    async def read_block_document_by_name(
        self,
        name: str,
        block_type_slug: str,
        include_secrets: bool = True,
    ) -> "BlockDocument":
        """
        Read the block document with the specified name that corresponds to a
        specific block type name.

        Args:
            name: The block document name.
            block_type_slug: The block type slug.
            include_secrets (bool): whether to include secret values
                on the Block, corresponding to Pydantic's `SecretStr` and
                `SecretBytes` fields. These fields are automatically obfuscated
                by Pydantic, but users can additionally choose not to receive
                their values from the API. Note that any business logic on the
                Block may not work if this is `False`.

        Raises:
            httpx.RequestError: if the block document was not found for any reason

        Returns:
            A block document or None.
        """
        try:
            response = await self.request(
                "GET",
                "/block_types/slug/{slug}/block_documents/name/{block_document_name}",
                path_params={"slug": block_type_slug, "block_document_name": name},
                params=dict(include_secrets=include_secrets),
            )
        except HTTPStatusError as e:
            if e.response.status_code == 404:
                raise ObjectNotFound(http_exc=e) from e
            else:
                raise
        from prefect_cloud.schemas.objects import BlockDocument

        return BlockDocument.model_validate(response.json())

    async def read_block_type_by_slug(self, slug: str) -> "BlockType":
        """
        Read a block type by its slug.
        """
        try:
            response = await self.request(
                "GET",
                "/block_types/slug/{slug}",
                path_params={"slug": slug},
            )
        except HTTPStatusError as e:
            if e.response.status_code == 404:
                raise ObjectNotFound(http_exc=e) from e
            else:
                raise
        from prefect_cloud.schemas.objects import BlockType

        return BlockType.model_validate(response.json())

    async def get_most_recent_block_schema_for_block_type(
        self,
        block_type_id: "UUID",
    ) -> "BlockSchema | None":
        """
        Fetches the most recent block schema for a specified block type ID.

        Args:
            block_type_id: The ID of the block type.

        Raises:
            httpx.RequestError: If the request fails for any reason.

        Returns:
            The most recent block schema or None.
        """
        try:
            response = await self.request(
                "POST",
                "/block_schemas/filter",
                json={
                    "block_schemas": {"block_type_id": {"any_": [str(block_type_id)]}},
                    "limit": 1,
                },
            )
        except HTTPStatusError:
            raise
        from prefect_cloud.schemas.objects import BlockSchema

        return next(iter(validate_list(BlockSchema, response.json())), None)
