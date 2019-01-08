from benchmarks import REGISTERED, VERSIONBY

apps = list()
for app in REGISTERED:
    for version in VERSIONBY:
        apps.append(app(version_by=version))
        apps[-1].start(version)
