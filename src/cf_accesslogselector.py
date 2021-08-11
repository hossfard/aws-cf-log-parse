from cf_accesslog import AccessLog



class AccessLogSelector():
    def __init__(self, columns, store):
        self.columns = columns
        self.conditions = {}
        self.store = store
        self.drange = None

    def where(self, conditions):
        '''Specify selection queries via a key-value store

        The keys are the column names of the access file, and the
        values are regex expressions

        @param {dict} conditions list of conditions
        @return {AccessLogSelector} self

        '''
        self.conditions = conditions
        return self

    def daterange(self, drange):
        '''Specify date range

        All dates must be in YYYY-mm-dd format

        @param {list} drange a 2-tuple containing dates
        @return {AccessLogSelector} self

        '''
        self.drange = drange
        return self

    def execute(self):
        '''Run the generated query and return the results

        @return {AccessLogQuery} results matching the query or None
        '''

        if self.drange is None:
            list_keys = self.store.list_keys()
        else:
            list_keys = self.store.list_keys(date_range=self.drange)

        ret = None
        for k in list_keys:
            log = self.store.access_log(k)
            log_q = log.select(self.columns, self.conditions)
            if ret is None:
                ret = log_q
            else:
                ret = ret.concatenate(log_q)
        return ret
