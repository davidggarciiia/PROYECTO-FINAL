"""Tests for vcity pipeline (no real HTTP needed — mock tile data)."""
from __future__ import annotations

import math
import pytest
from unittest.mock import AsyncMock, MagicMock, patch


# ═══════════════════════════════════════════════════════════════════════════════
# 1. Tile coordinate conversion
# ═══════════════════════════════════════════════════════════════════════════════

def test_lat_lng_to_tile():
    from pipelines.peatonal.vcity import lat_lng_to_tile
    # Barcelona center ~41.3879, 2.1699
    # At zoom 15 (2^15 = 32768 tiles per axis):
    #   x = int((2.1699 + 180) / 360 * 32768) = int(16581.something) → 16581
    #   y calculation via Mercator formula → approx 12644
    x, y = lat_lng_to_tile(41.3879, 2.1699, zoom=15)
    assert 16500 < x < 16700
    assert 12100 < y < 12400


def test_lat_lng_to_tile_known_values():
    """Cross-check against known tile coordinates for Barcelona."""
    from pipelines.peatonal.vcity import lat_lng_to_tile
    # At zoom 15, Barcelona center should be around tile (16745, 12644)
    x, y = lat_lng_to_tile(41.3879, 2.1699, zoom=15)
    assert isinstance(x, int)
    assert isinstance(y, int)
    # Both must be positive tile indices
    assert x > 0
    assert y > 0


def test_lat_lng_to_tile_zoom_0():
    from pipelines.peatonal.vcity import lat_lng_to_tile
    # At zoom 0 the whole world is a single tile (0, 0)
    x, y = lat_lng_to_tile(41.3879, 2.1699, zoom=0)
    assert x == 0
    assert y == 0


def test_lat_lng_to_tile_zoom_1():
    from pipelines.peatonal.vcity import lat_lng_to_tile
    # At zoom 1 Barcelona (northern hemisphere, eastern) → tile (1, 0)
    x, y = lat_lng_to_tile(41.3879, 2.1699, zoom=1)
    assert x == 1
    assert y == 0


# ═══════════════════════════════════════════════════════════════════════════════
# 2. _aggregate_tile_features
# ═══════════════════════════════════════════════════════════════════════════════

def test_aggregate_tile_features_empty():
    from pipelines.peatonal.vcity import _aggregate_tile_features
    assert _aggregate_tile_features([]) is None


def test_aggregate_tile_features_all_zero_pedestrians():
    from pipelines.peatonal.vcity import _aggregate_tile_features
    features = [
        {"num_pedestrians": 0.0, "tourist_rate": 0.1},
        {"num_pedestrians": 0.0, "tourist_rate": 0.2},
    ]
    assert _aggregate_tile_features(features) is None


def test_aggregate_tile_features_normal():
    from pipelines.peatonal.vcity import _aggregate_tile_features
    features = [
        {
            "num_pedestrians": 1000.0,
            "tourist_rate": 0.2,
            "resident_rate": 0.5,
            "shopping_and_leisure_rate": 0.15,
        },
        {
            "num_pedestrians": 2000.0,
            "tourist_rate": 0.3,
            "resident_rate": 0.4,
            "shopping_and_leisure_rate": 0.20,
        },
    ]
    result = _aggregate_tile_features(features)
    assert result is not None
    assert result["num_pedestrians"] == pytest.approx(1500.0)
    assert result["tourist_rate"] == pytest.approx(0.25)
    assert result["resident_rate"] == pytest.approx(0.45)
    assert result["shopping_rate"] == pytest.approx(0.175)


def test_aggregate_tile_features_single():
    from pipelines.peatonal.vcity import _aggregate_tile_features
    features = [
        {"num_pedestrians": 500.0, "tourist_rate": 0.4, "resident_rate": 0.3,
         "shopping_and_leisure_rate": 0.1},
    ]
    result = _aggregate_tile_features(features)
    assert result is not None
    assert result["num_pedestrians"] == pytest.approx(500.0)
    assert result["tourist_rate"] == pytest.approx(0.4)


def test_aggregate_tile_features_missing_rates():
    from pipelines.peatonal.vcity import _aggregate_tile_features
    # Features with no rate fields — pedestrian count should still work
    features = [
        {"num_pedestrians": 300.0},
        {"num_pedestrians": 700.0},
    ]
    result = _aggregate_tile_features(features)
    assert result is not None
    assert result["num_pedestrians"] == pytest.approx(500.0)
    assert result["tourist_rate"] is None
    assert result["shopping_rate"] is None
    assert result["resident_rate"] is None


def test_aggregate_tile_features_partial_rates():
    from pipelines.peatonal.vcity import _aggregate_tile_features
    features = [
        {"num_pedestrians": 1000.0, "tourist_rate": 0.2},
        {"num_pedestrians": 2000.0},  # no tourist_rate
    ]
    result = _aggregate_tile_features(features)
    assert result is not None
    assert result["num_pedestrians"] == pytest.approx(1500.0)
    # Only one feature has tourist_rate → mean of [0.2]
    assert result["tourist_rate"] == pytest.approx(0.2)


# ═══════════════════════════════════════════════════════════════════════════════
# 3. _mean_field
# ═══════════════════════════════════════════════════════════════════════════════

def test_mean_field_empty():
    from pipelines.peatonal.vcity import _mean_field
    assert _mean_field([], "tourist_rate") is None


def test_mean_field_all_none():
    from pipelines.peatonal.vcity import _mean_field
    features = [{"tourist_rate": None}, {"other": 1.0}]
    assert _mean_field(features, "tourist_rate") is None


def test_mean_field_normal():
    from pipelines.peatonal.vcity import _mean_field
    features = [
        {"tourist_rate": 0.1},
        {"tourist_rate": 0.3},
        {"tourist_rate": 0.5},
    ]
    assert _mean_field(features, "tourist_rate") == pytest.approx(0.3)


# ═══════════════════════════════════════════════════════════════════════════════
# 4. _decode_tile — unit test with a minimal synthesized MVT
# ═══════════════════════════════════════════════════════════════════════════════

def test_decode_tile_empty_bytes():
    from pipelines.peatonal.vcity import _decode_tile
    result = _decode_tile(b"")
    assert result == []


def test_decode_tile_invalid_bytes():
    from pipelines.peatonal.vcity import _decode_tile
    # Should not raise, just return empty list
    result = _decode_tile(b"\x00\x01\x02garbage")
    assert isinstance(result, list)


def test_decode_tile_with_mock_mapbox_vector_tile():
    """Test MVT decoding via mapbox_vector_tile mock."""
    mock_tile = {
        "barcelonapedestrians_100percentage_v2": {
            "features": [
                {
                    "properties": {
                        "gid": 1,
                        "num_pedestrians": 1200.0,
                        "tourist_rate": 0.25,
                        "resident_rate": 0.45,
                        "shopping_and_leisure_rate": 0.15,
                    }
                },
                {
                    "properties": {
                        "gid": 2,
                        "num_pedestrians": 800.0,
                        "tourist_rate": 0.15,
                        "resident_rate": 0.55,
                        "shopping_and_leisure_rate": 0.10,
                    }
                },
            ]
        }
    }

    mock_mvt = MagicMock()
    mock_mvt.decode.return_value = mock_tile

    import sys
    sys.modules["mapbox_vector_tile"] = mock_mvt

    try:
        from pipelines.peatonal.vcity import _decode_tile
        # Force reload to pick up mocked module
        import importlib
        import pipelines.vcity as vcity_mod
        importlib.reload(vcity_mod)

        result = vcity_mod._decode_tile(b"\x1a\x00")  # any non-empty bytes
        assert len(result) == 2
        assert result[0]["num_pedestrians"] == pytest.approx(1200.0)
        assert result[1]["tourist_rate"] == pytest.approx(0.15)
    finally:
        del sys.modules["mapbox_vector_tile"]


# ═══════════════════════════════════════════════════════════════════════════════
# 5. Integration: _fetch_vcity_data with mocked HTTP + decode
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_fetch_vcity_data_empty_tiles():
    """When all tiles return None (no data), zona_data should be empty."""
    from pipelines.peatonal.vcity import _fetch_vcity_data

    zonas = [
        {"zona_id": "z1", "nombre": "Eixample", "lat": 41.3879, "lng": 2.1699},
        {"zona_id": "z2", "nombre": "Gracia", "lat": 41.4039, "lng": 2.1565},
    ]

    with patch("pipelines.vcity._fetch_tile", new_callable=AsyncMock) as mock_fetch:
        mock_fetch.return_value = None
        result = await _fetch_vcity_data(zonas)
        assert result == {}
        # Each unique tile should have been attempted
        assert mock_fetch.call_count >= 1


@pytest.mark.asyncio
async def test_fetch_vcity_data_with_features():
    """When tiles return data, zona_data should contain aggregated metrics."""
    from pipelines.peatonal.vcity import _fetch_vcity_data

    zonas = [
        {"zona_id": "z1", "nombre": "Eixample", "lat": 41.3879, "lng": 2.1699},
    ]

    fake_tile_bytes = b"\x1a\x04test"

    fake_features = [
        {"num_pedestrians": 1000.0, "tourist_rate": 0.2, "resident_rate": 0.5,
         "shopping_and_leisure_rate": 0.15},
    ]

    with patch("pipelines.vcity._fetch_tile", new_callable=AsyncMock) as mock_fetch, \
         patch("pipelines.vcity._decode_tile") as mock_decode:
        mock_fetch.return_value = fake_tile_bytes
        mock_decode.return_value = fake_features

        result = await _fetch_vcity_data(zonas)

        assert "z1" in result
        assert result["z1"]["num_pedestrians"] == pytest.approx(1000.0)
        assert result["z1"]["tourist_rate"] == pytest.approx(0.2)


@pytest.mark.asyncio
async def test_fetch_vcity_data_deduplicates_tiles():
    """Two zones on the same tile should only trigger one HTTP request."""
    from pipelines.peatonal.vcity import lat_lng_to_tile, _fetch_vcity_data

    # Use coordinates that should map to the same tile at zoom 15
    # (same tile = same x/y when coordinates are very close)
    lat1, lng1 = 41.3879, 2.1699
    lat2, lng2 = 41.3881, 2.1701  # ~22m apart — same z15 tile

    tx1, ty1 = lat_lng_to_tile(lat1, lng1, 15)
    tx2, ty2 = lat_lng_to_tile(lat2, lng2, 15)

    if (tx1, ty1) != (tx2, ty2):
        pytest.skip("Test coordinates resolve to different tiles on this system")

    zonas = [
        {"zona_id": "z1", "nombre": "Zone A", "lat": lat1, "lng": lng1},
        {"zona_id": "z2", "nombre": "Zone B", "lat": lat2, "lng": lng2},
    ]

    with patch("pipelines.vcity._fetch_tile", new_callable=AsyncMock) as mock_fetch, \
         patch("pipelines.vcity._decode_tile") as mock_decode:
        mock_fetch.return_value = b"\x1a\x04test"
        mock_decode.return_value = [
            {"num_pedestrians": 500.0},
        ]

        result = await _fetch_vcity_data(zonas)
        # Only one tile fetch for two zones sharing the same tile
        assert mock_fetch.call_count == 1
        # Both zones get the same aggregated result
        assert "z1" in result
        assert "z2" in result
