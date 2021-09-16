class Datafile:

    def __init__(self, datafile, dark_start = None, dark_end = None, force_regularize=True):
        self.parser = AnalysisVisParser("%Y-%m-%d %H:%M:%S")
        self.id = None
        self.start_date = None
        self.end_date = None
        self.dark_start = dark_start
        self.dark_end = dark_end
        self.phase_change_indices = tuple()
        self.phase_change_dates = tuple()
        self.subject_freq = dict()
        self.freq = None
        self.allowed_agg_freq = None
        self.regular = True
        self.force_regularize = force_regularize
        # None, nudge or interpolate
        # TODO implement nudge
        self.regularization_method = regularization_method
        self.initialized = False
        self.num_observations = dict()
        self.subjects = []
        self.descriptors = ["subject", "date_time", "interval"]
        self.light_column = ["light"]
        self.parameters = []
        self.data = None
        self.subject_split_data = collections.OrderedDict()
        self.logger = logging.getLogger('clams-convert')
        self.__create(datafile)

    def __create(self, source):
        if isinstance(source, str):
            if os.path.exists(source):
                self.data = self.parser.parse(source)
        elif isinstance(source, pd.DataFrame):
            self.data = source
            self.data.date_time = pd.to_datetime(self.data.date_time,
                                                 format="%Y-%m-%d %H:%M:%S")
        else:
            raise ValueError("Datafile source is of unrecognized type, " +
                "only path and DataFrame are accepted.")
        self.__initialize()
        return self
