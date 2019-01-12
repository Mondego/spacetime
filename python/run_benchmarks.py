from benchmarks import REGISTERED, VERSIONBY

apps = list()
for appname, app in REGISTERED:
    for version in VERSIONBY:
        apps.append(app(
            version_by=version,
            instrument="benchmarks/results/{0}.main.{1}.tsv".format(
            appname, VERSIONBY[version])))
        apps[-1].start(version)
