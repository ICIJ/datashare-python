def pil_supported_extensions() -> set[str]:
    from passport_service.core.preprocessing import (  # noqa:PLC0415
        PIL_SUPPORTED_EXTENSIONS,
    )

    return set(PIL_SUPPORTED_EXTENSIONS)
