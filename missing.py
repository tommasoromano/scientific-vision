def missing_values(self, patient=None, code=None, component_code=None):
    '''
    Returns obsesrvations have missing values
    for columns valueQuantity_value or component_valueQuantity_value.
    
    Filtered by patient and code_coding_code or component_code_coding_code
    '''
    df = self.observations
    df = df.select(
        'subject_reference',
        'code_coding_code',
        'component_code_coding_code',
        'valueQuantity_value',
        'component_valueQuantity_value'
    )
    if patient:
        df = df.filter(F.col('subject_reference').contains(patient))
    if code:
        df = df.filter(F.col('code_coding_code').contains(code))
    if component_code:
        df = df.filter(F.col('component_code_coding_code').contains(component_code))
        
    return df.filter(
        (F.col('valueQuantity_value').isNull()) &
        (F.col('component_valueQuantity_value').isNull())
        )

def missing_values_filter(self, patients=None):
    '''
    - patients: str or list

    Returns a list of (patient, code, component_code)
    '''
    df = self.observations
    missing_count_filter = 1
    msng_dct = dict()
    for p in patients:
        dfp = df.filter(F.col('subject_reference').contains(p))
        filters = [
                (F.col('code_coding_code').isNotNull()) & 
                (F.col('component_code_coding_code').isNull()) &
                (F.col('valueQuantity_value').isNull()),
                (F.col('code_coding_code').isNotNull()) & 
                (F.col('component_code_coding_code').isNotNull()) &
                (F.col('component_valueQuantity_value').isNull()),
                (F.col('code_coding_code').isNull()) & 
                (F.col('component_code_coding_code').isNotNull()) &
                (F.col('component_valueQuantity_value').isNull())
        ]
        i = 0
        for f in filters:
            tmp = dfp.filter(f)
            res = tmp.groupBy('code_coding_code','component_code_coding_code').count()
            res = res.filter(res['count'] > missing_count_filter)
            if res.count() > 0:
                if p not in msng_dct.keys():
                    msng_dct[p] = dict()
                msng_dct[p][i] = [(r['code_coding_code'],r['component_code_coding_code'],r['count']) for r in res.collect()]
    return

def missing_brute_force(self):
    relations = self.get_alls(self.observations, 
        ['subject_reference','code_coding_code','component_code_coding_code'])
    rows = relations.collect()
    df = self.observations
    df = df.select(
        'subject_reference',
        'code_coding_code',
        'component_code_coding_code',
        'valueQuantity_value',
        'component_valueQuantity_value'
    )
    for r in rows:
        ptnt = F.col('subject_reference').isNull() if r['subject_reference'] == None else df['subject_reference'] == r['subject_reference']
        cd = F.col('code_coding_code').isNull() if r['code_coding_code'] == None else df['code_coding_code'] == r['code_coding_code']
        cmp = F.col('component_code_coding_code').isNull() if r['component_code_coding_code'] == None else df['component_code_coding_code'] == r['component_code_coding_code']
        tmp = df.filter(
            (ptnt) &
            (cd) & 
            (cmp)
        )
        filters = [
                (F.col('code_coding_code').isNotNull()) & 
                (F.col('component_code_coding_code').isNull()) &
                (F.col('valueQuantity_value').isNull()),
                (F.col('code_coding_code').isNotNull()) & 
                (F.col('component_code_coding_code').isNotNull()) &
                (F.col('component_valueQuantity_value').isNull()),
                (F.col('code_coding_code').isNull()) & 
                (F.col('component_code_coding_code').isNotNull()) &
                (F.col('component_valueQuantity_value').isNull())
        ]
        for f in filters:
            dff = tmp.filter(f)
            if dff.count() > 0:
                print(r, tmp.count(), dff.count())
    return