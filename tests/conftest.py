"""Pytest configuration and fixtures for devbackup tests."""

from hypothesis import settings, Phase

# Configure hypothesis to use fewer examples for faster test runs
# Disable shrinking phase to speed up tests further
settings.register_profile(
    "fast", 
    max_examples=2, 
    deadline=10000,
    phases=[Phase.explicit, Phase.reuse, Phase.generate]
)
settings.register_profile("ci", max_examples=50, deadline=10000)
settings.register_profile("dev", max_examples=2, deadline=10000)

# Use the fast profile by default
settings.load_profile("fast")
