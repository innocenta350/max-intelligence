"""MAX Intelligence test package.

Stage conventions (M1 plan §G): layer tests use committed synthetic mini-GTFS
fixtures; live-feed tests carry the pre-registered ``live`` marker and are
gated by ``LIVE_FEED_TESTS=1`` (see pytest.ini and AGENTS.md §7).
"""
