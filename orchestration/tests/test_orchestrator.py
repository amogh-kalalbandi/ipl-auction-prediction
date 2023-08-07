"""Unit test file for orchestrator."""

import training_orchestrator


def test_pull_files_from_s3():
    """Testing pulling from files from S3 bucket."""
    expected_filename_list = ["tmp/training_data/training_data_2021.csv"]

    actual_filename_list = training_orchestrator.get_input_files_from_s3()

    expected_filename_list.sort()
    actual_filename_list.sort()

    assert actual_filename_list == expected_filename_list


def test_dictionary_vectorizer_method():
    """Test dictionary vectorizers generated."""
    training_number_of_columns = 193
    filename_list = ["tmp/training_data/training_data_2021.csv"]

    dict_vectorizer = training_orchestrator.prepare_dictionary_vectorizers(
        filename_list
    )

    assert training_number_of_columns == dict_vectorizer["train"].num_row()
