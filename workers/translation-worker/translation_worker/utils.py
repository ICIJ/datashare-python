from .constants import TorchDevice


def find_device(device_name: str = TorchDevice.CPU) -> TorchDevice.CPU:
    """Check if a device is available; if not, return cpu

    :param device_name: device name
    :return: str device name
    """
    import torch  # noqa: PLC0415

    if (
        hasattr(torch.backends, device_name)
        and hasattr(getattr(torch.backends, device_name), "is_available")
        and getattr(torch.backends, device_name).is_available()
    ):
        return device_name

    return TorchDevice.CPU
