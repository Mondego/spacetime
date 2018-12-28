from benchmarks import REGISTERED

apps = list()
for app in REGISTERED:
    apps.append(app())
    apps[-1].start()

for app in apps:
    app.join()