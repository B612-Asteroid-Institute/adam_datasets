from adam_datasets.sources import generate_file_crc32c


def test_crc32c():
    """
    Test that the crc32c function produces the same as Google Cloud Strorage
    """
    assert (
        generate_file_crc32c("adam_datasets/test/test_data/test_file.bin") == "2LOVRw=="
    ), "crc32c checksum does not match one returned by google cloud storage"
