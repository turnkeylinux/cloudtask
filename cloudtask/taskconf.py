class TaskConf:
    user = 'root'

    command = None

    pre = None
    post = None
    overlay = None

    hub_apikey = None
    timeout = None

    split = None
    workers = []

    ec2_region = 'us-east-1'
    ec2_size = 'm1.small'
    ec2_type = 's3'

    __all__ = [ attr for attr in dir() if not attr.startswith("_") ]

    def __getitem__(self, name):
        return getattr(self, name)

    def __setitem__(self, name, val):
        return setattr(self, name, val)

    def __repr__(self):
        return `self.dict()`

    def dict(self):
        d = {}
        for attr in self.__all__:
            d[attr] = getattr(self, attr)
        return d

    @classmethod
    def fromdict(cls, d):
        taskconf = cls()
        for attr in d:
            taskconf[attr] = d[attr]
        return taskconf

    @property
    def ec2_opts(self):
        opts = dict([ (attr[4:], self[attr]) 
                       for attr in self.__all__ 
                       if attr.startswith('ec2_') ])
        opts['label'] = 'Cloudtask: ' + self.command
        return opts
