def test_package_imports():
    import seedgen  # noqa: F401
    from seedgen import (  # noqa: F401
        errors, prng, models, versions, canonical, generator,
        storage, repository, audit, auth, manager, api, config,
    )
