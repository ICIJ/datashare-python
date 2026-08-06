def pil_supporter_extensions() -> set[str]:
    from passport_service.core.preprocessing import PIL_SUPPORTED_EXTENSIONS

    return set(PIL_SUPPORTED_EXTENSIONS)
