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

    def init_start_end_date(self, round_mins=True):
        self.start_date = sorted(self.data.date_time)[0]
        self.end_date = sorted(self.data.date_time)[-1]
        if round_mins:
            self.start_date = round_minutes(self.start_date)
            self.end_date = round_minutes(self.end_date)

    def init_subjects(self, rename_subject_mapping):
        if rename_subject_mapping is not None:
            self.rename_subjects(rename_subject_mapping)
        self.subjects = list(set(self.data.subject.tolist()))
        for s in self.subjects:
            self.subject_split_data[s] = self.data.query('subject == @s')

    def init_num_observations(self):
        try:
            self.num_observations = {k: s.shape[0] for (k, s) in self.subject_split_data.items()}
        except ValueError:
            print("Subject data not initialized.")

    def init_parameters(self):
        self.parameters = self.get_parameters()

    def init_light_column(self):
        if self.dark_start is None or self.dark_end is None:
            raise ValueError("Light column is not present in the data " +
                "and dark_start or dark_end attributes are not set.")
        else:
            ts = self.data['date_time'].time()
            ds = str_to_time(self.dark_start)
            de = str_to_time(self.dark_end)
            # TODO this will not work
            self.data['light'] = pd.Series(np.where(ts >= ds and ts < de, 0, 1))

    def init_phase_changes(self):
        if self.data['light'].iloc[0] == 0:
            next_phase = 1
        elif self.data['light'].iloc[0] == 1:
            next_phase = 0
        else:
            raise ValueError("Illegal value in the light column, only 0 and 1 are permitted.")
        i1 = list(self.data['light']).index(next_phase)
        i2 = list(self.data['light'].iloc[(i1+1):]).index(1-next_phase)
        self.phase_change_indices = (i1, i2)
        self.phase_change_dates = (self.data.date_time.iloc[i1],
                                   self.data.date_time.iloc[i2])

    def init_freq(self):
        try:
            interval_lengths = self.subject_interval_lengths()
            subject_freq = self.find_freq(interval_lengths)
            self.validate_freq(subject_freq)
            self.freq = freq_to_seconds(list(set(subject_freq.values()))[0])
        except ValueError:
            print("Subject data not initialized.")


    def init_allowed_agg_freq(self):
        phase1_duration = self.phase_change_dates[1] - self.phase_change_dates[0]
        phase2_duration = timedelta(hours=24) - phase1_duration
        common_agg = find_common_divisors(phase1_duration.total_seconds(),
                                          phase2_duration.total_seconds())
        self.allowed_agg_freq = divisible(common_agg, self.freq)
