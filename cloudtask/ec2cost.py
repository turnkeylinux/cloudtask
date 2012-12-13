class Error(Exception):
    pass

class AttrDict(dict):
    def __getattr__(self, name):
        if name in self:
            return self[name]
        raise AttributeError("no such attribute '%s'" % name)

    def __setattr__(self, name, val):
        self[name] = val

class Cost(AttrDict):
    def __init__(self, region, type, size, hourly, upfront=0, reserved=0):
        self.region = region
        self.type = type
        self.size = size
        self.hourly = hourly
        self.upfront = upfront
        self.reserved = reserved

    @property
    def monthly(self):
        return (self.hourly * 24) * 30

    @property
    def year_1(self):
        if self.reserved == 0:
            return self.monthly * 12

        return (self.monthly * 12) + (self.upfront / self.reserved)

    @property
    def year_3(self):
        return self.year_1 * 3

    @property
    def human_size(self):
        return self.size.split(".")[1].capitalize()

    @property
    def human_hourly(self):
        #Micro: $0.03/hour (reserved)
        s = "%s: $%s/hour" % (self.human_size, self.hourly)
        s += " (reserved)" if self.reserved > 0 else ""
        return s

    @property
    def human_upfront(self):
        #1 year: $220 one-time payment
        if self.reserved == 1:
            return "1 year: $%d up-front investment" % self.upfront

        if self.reserved == 3:
            return "3 years: $%d up-front investment" % self.upfront

        return "$0"

class Costs:
    def __init__(self):
        self.costs = []

    def add(self, region, type, size,
            od_h, y1_u=None, y1_h=None, y3_u=None, y3_h=None):
        """add ec2 cost
            region: region codename (e.g., us-east-1)
            type: instance backed type (ebs | s3)
            size: instance size (e.g., m1.small)
            od_h: on-demand hourly cost
            y1_u: reserved 1 year upfront cost
            y1_h: reserved 1 year hourly cost
            y3_u: reserved 3 year upfront cost
            y3_h: reserved 3 year hourly cost
        """
        self.costs.append(Cost(region, type, size, od_h))

        if y1_u and y1_h:
            self.costs.append(Cost(region, type, size, y1_h, y1_u, 1))
            
        if y3_u and y3_h:
            self.costs.append(Cost(region, type, size, y3_h, y3_u, 3))

    def get(self, region, size, type, reserved=0):
        """get ec2 cost matching region, size, type, optionally reserved years

            from ec2cost import costs

            # regular
            c = costs.get("us-east-1", "m1.small", "s3")
            c = costs.get("us-east-1", "m1.small", "ebs")

            # reserved instance for 1 year
            c1 = costs.get("us-west-1", "t1.micro", "ebs", reserved=1)

            # reserved instance for 3 years
            c = costs.get("eu-west-1", "c1.medium", "ebs", reserved=3)
        """
        for c in self.costs:
            if c.region == region and \
               c.size == size and \
               c.type == type and \
               c.reserved == reserved:
                
                return c

        raise Error("No matching cost")

# generate costs (reservation costs are for "medium utilization")

costs = Costs()
for region in ("us-east-1", "us-west-2"):
    costs.add(region, "s3", "m1.small", 0.094)
    costs.add(region, "s3", "c1.medium", 0.187)
    costs.add(region, "ebs", "t1.micro", 0.020, 54.0, 0.007, 82.0, 0.007)
    costs.add(region, "ebs", "m1.small", 0.080, 160.0, 0.024, 250.0, 0.019)
    costs.add(region, "ebs", "c1.medium", 0.165, 415.0, 0.060, 638.0, 0.053)

for region in ("us-west-1", "eu-west-1", "ap-southeast-1"):
    costs.add(region, "s3", "m1.small", 0.105)
    costs.add(region, "s3", "c1.medium", 0.209)
    costs.add(region, "ebs", "t1.micro", 0.025, 54.0, 0.010, 82.0, 0.010)
    costs.add(region, "ebs", "m1.small", 0.090, 160.0, 0.031, 250.0, 0.025)
    costs.add(region, "ebs", "c1.medium", 0.186, 415.0, 0.080, 638.0, 0.070)

region = "ap-northeast-1"
costs.add(region, "s3", "m1.small", 0.105)
costs.add(region, "s3", "c1.medium", 0.210)
costs.add(region, "ebs", "t1.micro", 0.027, 57.0, 0.011, 86.0, 0.011)
costs.add(region, "ebs", "m1.small", 0.092, 168.0, 0.036, 262.5, 0.029)
costs.add(region, "ebs", "c1.medium", 0.190, 436.0, 0.090, 670.0, 0.080)

region = "sa-east-1"
costs.add(region, "s3", "m1.small", 0.126)
costs.add(region, "s3", "c1.medium", 0.250)
costs.add(region, "ebs", "t1.micro", 0.027, 73.0, 0.009, 111.0, 0.009)
costs.add(region, "ebs", "m1.small", 0.115, 307.13, 0.040, 473.0, 0.031)
costs.add(region, "ebs", "c1.medium", 0.230, 614.0, 0.080, 945.0, 0.070)

