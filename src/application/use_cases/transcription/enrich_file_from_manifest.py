from kink import di
from src.application.dtos.request.transcription import ManifestEnrichmentRequestDTO
from src.application.mappers.blob_file_reference_mapper import (
    to_blob_file_reference_entity,
)
from src.application.mappers.files_processing_enrichment_mapper import (
    to_files_xlsx_enrichment_entity,
)
from src.config.logger import logger
from src.domain.exceptions.ingestion_exceptions import BatchNotFound, InitiativeNotFound
from src.domain.ports.application.transcription import EnrichFileFromManifestUseCase
from src.domain.ports.infrastructure.blob_storage.blob_storage import BlobStorage
from src.domain.ports.infrastructure.persistence.call_processing import CallProcessing
from src.domain.ports.infrastructure.persistence.files_processing import FilesProcessing
from src.domain.ports.infrastructure.persistence.initiatives import Initiatives
from src.application.parsers.xlsx_manifest_reader import norm_wav_key, parse_xlsx_manifest

class EnrichFileFromManifest(EnrichFileFromManifestUseCase):
    def __init__(
        self,
        blob_storage_adapter: BlobStorage | None = None,
        files_processing_repo: FilesProcessing | None = None,
        call_processing_repo: CallProcessing | None = None,
        initiative_repo: Initiatives | None = None,
    ) -> None:
        self._blob_storage_adapter = blob_storage_adapter or di[BlobStorage]()
        self._files_repo = files_processing_repo or di[FilesProcessing]()
        self._call_repo = call_processing_repo or di[CallProcessing]()
        self._initiative_repo = initiative_repo or di[Initiatives]()

    async def execute(self, manifest: ManifestEnrichmentRequestDTO) -> None:
        if not manifest.xlsx_name:
            return
        batch = await self._call_repo.get_by_id(manifest.batch_id)
        if not batch:
            raise BatchNotFound(manifest.batch_id)
        initiative_info = await self._initiative_repo.get_by_name(initiative=manifest.initiative_id)
        if not initiative_info:
            raise InitiativeNotFound(manifest.initiative_id)
        xlsx_file = await self._blob_storage_adapter.download_file(
            to_blob_file_reference_entity(
                account_name=initiative_info.storage.accountName,
                account_key=initiative_info.storage.accountKey,
                container_name=batch.storage_container,
                blob_path=(
                    f"{manifest.initiative_id}/"
                    f"{manifest.batch_id.split(':')[-1]}/"
                    f"{manifest.xlsx_name}"
                ),
            )
        )
        parsed_manifest = parse_xlsx_manifest(xlsx_file)
        key = norm_wav_key(manifest.file_name)
        row = parsed_manifest.get(key) if key else None
        if not row:
            logger.warning("Archivo no encontrado en manifiesto XLSX", context={
                "file_id": manifest.file_id,
                "batch_id": manifest.batch_id,
                "file_name": manifest.file_name,
                "xlsx_name": manifest.xlsx_name,
            })
            return
        await self._files_repo.apply_xlsx_enrichment(
            manifest.file_id,
            to_files_xlsx_enrichment_entity(row),
        )
        logger.info("Enriquecimiento de manifiesto aplicado", context={
            "file_id": manifest.file_id,
            "batch_id": manifest.batch_id,
            "xlsx_name": manifest.xlsx_name,
        })
