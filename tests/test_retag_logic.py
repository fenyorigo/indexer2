from app.maintenance.retag import load_map, rewrite_tags


def test_rewrite_tags_replaces_exact_matches() -> None:
    after, changed = rewrite_tags(
        ["Veronika", "Family"],
        {"Veronika": "Baján Veronika (Veronika)"},
        False,
    )
    assert changed is True
    assert after == ["Baján Veronika (Veronika)", "Family"]


def test_rewrite_tags_replaces_only_hierarchical_leaf() -> None:
    after, changed = rewrite_tags(
        ["People|Veronika", "Places|Budapest"],
        {"Veronika": "Baján Veronika (Veronika)"},
        False,
        hierarchical=True,
    )
    assert changed is True
    assert after == ["People|Baján Veronika (Veronika)", "Places|Budapest"]


def test_rewrite_tags_dedupes_rewritten_values() -> None:
    after, changed = rewrite_tags(
        ["Bori", "Balogh Borbála (Bori)"],
        {"Bori": "Balogh Borbála (Bori)"},
        False,
    )
    assert changed is True
    assert after == ["Balogh Borbála (Bori)"]


def test_load_map_accepts_headerless_csv(tmp_path) -> None:
    csv_path = tmp_path / "map.csv"
    csv_path.write_text("old,new\nfoo,bar\n", encoding="utf-8")

    mapping = load_map(csv_path, False)

    assert mapping == {"foo": "bar"}
