import run_pipeline


def test_get_stages_builds_expected_commands_without_running_them() -> None:
    stages = run_pipeline.get_stages()

    assert [name for name, _ in stages] == [
        "Extract HDF5 to Parquet",
        "Build dbt models",
        "Run dbt data-quality tests",
    ]
    assert stages[0][1] == [
        run_pipeline.PYTHON,
        "-m",
        "src.processing.extract_to_parquet",
    ]
    assert stages[1][1] == [
        str(run_pipeline.DBT),
        "run",
        "--project-dir",
        str(run_pipeline.DBT_PROJECT),
        "--profiles-dir",
        str(run_pipeline.DBT_PROFILES),
    ]
    assert stages[2][1] == [
        str(run_pipeline.DBT),
        "test",
        "--project-dir",
        str(run_pipeline.DBT_PROJECT),
        "--profiles-dir",
        str(run_pipeline.DBT_PROFILES),
    ]
