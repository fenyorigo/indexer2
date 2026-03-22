from app.cli import build_parser


def test_retag_arguments_parse() -> None:
    args = build_parser().parse_args(
        [
            "--cli",
            "--db",
            "/tmp/photos.db",
            "--media-root",
            "/tmp/photos",
            "--retag-map",
            "/tmp/tag-map.csv",
            "--retag-apply",
            "--retag-case-insensitive",
            "--retag-no-video",
            "--retag-ext",
            ".xmp",
        ]
    )

    assert str(args.retag_map) == "/tmp/tag-map.csv"
    assert args.retag_apply is True
    assert args.retag_case_insensitive is True
    assert args.retag_no_video is True
    assert args.retag_ext == [".xmp"]
