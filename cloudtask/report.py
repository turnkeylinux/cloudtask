
def report(session):
    taskconf = session.taskconf
    if not taskconf.report:
        return
