from pathlib import Path

from dxf2geo import extract


def test_extract_calls_subprocess(tmp_path: Path, mocker):
    dxf_path = tmp_path / "test.dxf"
    dxf_path.write_text("dummy dxf content")

    output_dir = tmp_path / "output"

    mock_run = mocker.patch("subprocess.run")
    extract.extract_geometries(dxf_path, output_dir)

    assert mock_run.called
    called_args = mock_run.call_args[0][0]
    assert "ogr2ogr" in called_args
    assert str(dxf_path) in called_args
