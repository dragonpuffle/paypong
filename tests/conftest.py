def pytest_addoption(parser):
    parser.addoption("--strict-integration", action="store_true")


def pytest_sessionfinish(session, exitstatus):
    if session.config.getoption("--strict-integration"):
        reporter = session.config.pluginmanager.get_plugin("terminalreporter")
        if reporter and reporter.stats.get("skipped"):
            session.exitstatus = 1
