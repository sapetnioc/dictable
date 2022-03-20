import datetime
import sqlite3

from dictable import Dictsqlite

def test_filters(dictdb):
    list_datetime = [datetime.datetime(2018, 5, 23, 12, 41, 33, 540),
                        datetime.datetime(1981, 5, 8, 20, 0),
                        datetime.datetime(1899, 12, 31, 1, 2, 3)]

    dictdb.execute('CREATE TABLE file ('
        'name TEXT NOT NULL PRIMARY KEY,'
        '_json TEXT)')

    # session.add_field("collection1", 'format', field_type=FIELD_TYPE_STRING,
    #                     description=None, index=True)
    # session.add_field("collection1", 'strings', field_type=FIELD_TYPE_LIST_STRING,
    #                     description=None)
    # session.add_field("collection1", 'datetime', field_type=FIELD_TYPE_DATETIME,
    #                     description=None)
    # session.add_field("collection1", 'has_format', field_type=FIELD_TYPE_BOOLEAN,
    #                     description=None)

    files = dictdb['file']
    file_names = ('abc', 'bcd', 'def', 'xyz')
    for file in file_names:
        for date in list_datetime:
            for format, ext in (('NIFTI', 'nii'),
                                ('DICOM', 'dcm'),
                                ('Freesurfer', 'mgz')):
                document = dict(
                    name='/%s_%d.%s' % (file, date.year, ext),
                    format=format,
                    strings=list(file),
                    datetime=date,
                    has_format=True,
                )
                files.add(document)
            d = dict(name=document, strings=list(file))
            files[f'/{file}_{date.year}.none'] = {'strings': list(file)}

    for filter, expected in (
            ('format == "NIFTI"',
                set([
                    '/xyz_1899.nii',
                    '/xyz_2018.nii',
                    '/abc_2018.nii',
                    '/bcd_1899.nii',
                    '/bcd_2018.nii',
                    '/def_1899.nii',
                    '/abc_1981.nii',
                    '/def_2018.nii',
                    '/def_1981.nii',
                    '/bcd_1981.nii',
                    '/abc_1899.nii',
                    '/xyz_1981.nii'
                ])
                ),

            ('"b" IN strings',
                set([
                    '/bcd_2018.mgz',
                    '/abc_1899.mgz',
                    '/abc_1899.dcm',
                    '/bcd_1981.dcm',
                    '/abc_1981.dcm',
                    '/bcd_1981.mgz',
                    '/bcd_1899.mgz',
                    '/abc_1981.mgz',
                    '/abc_2018.mgz',
                    '/abc_2018.dcm',
                    '/bcd_2018.dcm',
                    '/bcd_1899.dcm',
                    '/abc_2018.nii',
                    '/bcd_1899.nii',
                    '/abc_1981.nii',
                    '/bcd_1981.nii',
                    '/abc_1899.nii',
                    '/bcd_2018.nii',
                    '/abc_1899.none',
                    '/bcd_1899.none',
                    '/bcd_1981.none',
                    '/abc_2018.none',
                    '/bcd_2018.none',
                    '/abc_1981.none'
                ])
                ),

            ('(format == "NIFTI" OR NOT format == "DICOM")',
                set([
                    '/xyz_1899.nii',
                    '/xyz_1899.mgz',
                    '/bcd_2018.mgz',
                    '/bcd_1899.nii',
                    '/bcd_2018.nii',
                    '/def_1899.nii',
                    '/bcd_1981.mgz',
                    '/abc_1981.nii',
                    '/def_2018.mgz',
                    '/abc_1899.nii',
                    '/def_1899.mgz',
                    '/xyz_1899.none',
                    '/abc_2018.nii',
                    '/def_1899.none',
                    '/bcd_1899.mgz',
                    '/def_2018.nii',
                    '/abc_1981.mgz',
                    '/abc_1899.none',
                    '/xyz_1981.mgz',
                    '/bcd_1981.nii',
                    '/xyz_1981.nii',
                    '/abc_2018.mgz',
                    '/xyz_2018.nii',
                    '/abc_1899.mgz',
                    '/def_1981.nii',
                    '/def_1981.mgz',
                    '/bcd_1899.none',
                    '/xyz_2018.mgz',
                    '/bcd_1981.none',
                    '/xyz_1981.none',
                    '/abc_1981.none',
                    '/def_2018.none',
                    '/xyz_2018.none',
                    '/abc_2018.none',
                    '/def_1981.none',
                    '/bcd_2018.none'
                ])
                ),

            ('"a" IN strings',
                set([
                    '/abc_1899.none',
                    '/abc_1899.nii',
                    '/abc_2018.nii',
                    '/abc_1899.mgz',
                    '/abc_1899.dcm',
                    '/abc_1981.dcm',
                    '/abc_1981.nii',
                    '/abc_1981.mgz',
                    '/abc_2018.mgz',
                    '/abc_2018.dcm',
                    '/abc_2018.none',
                    '/abc_1981.none'
                ])
                ),

            ('NOT "b" IN strings',
                set([
                    '/xyz_1899.nii',
                    '/xyz_2018.dcm',
                    '/def_1981.dcm',
                    '/xyz_2018.nii',
                    '/xyz_1981.dcm',
                    '/def_1899.none',
                    '/xyz_1899.dcm',
                    '/xyz_1981.nii',
                    '/def_1899.dcm',
                    '/def_1899.nii',
                    '/def_2018.mgz',
                    '/def_2018.nii',
                    '/xyz_1899.mgz',
                    '/def_2018.dcm',
                    '/def_1899.mgz',
                    '/def_1981.mgz',
                    '/xyz_1981.mgz',
                    '/xyz_2018.mgz',
                    '/xyz_1899.none',
                    '/def_1981.nii',
                    '/xyz_2018.none',
                    '/xyz_1981.none',
                    '/def_2018.none',
                    '/def_1981.none'
                ])
                ),
            ('("a" IN strings OR NOT "b" IN strings)',
                set([
                    '/xyz_1899.nii',
                    '/xyz_1899.mgz',
                    '/def_1899.nii',
                    '/abc_1981.nii',
                    '/def_2018.mgz',
                    '/abc_1899.nii',
                    '/def_1899.mgz',
                    '/abc_2018.dcm',
                    '/xyz_1899.none',
                    '/xyz_2018.dcm',
                    '/def_1981.dcm',
                    '/abc_2018.nii',
                    '/def_1899.none',
                    '/abc_1981.dcm',
                    '/def_2018.nii',
                    '/abc_1981.mgz',
                    '/def_2018.dcm',
                    '/abc_1899.none',
                    '/xyz_1981.mgz',
                    '/xyz_1899.dcm',
                    '/abc_1899.dcm',
                    '/def_1899.dcm',
                    '/xyz_1981.nii',
                    '/abc_2018.mgz',
                    '/xyz_2018.nii',
                    '/abc_1899.mgz',
                    '/xyz_1981.dcm',
                    '/def_1981.nii',
                    '/def_1981.mgz',
                    '/xyz_2018.mgz',
                    '/xyz_1981.none',
                    '/abc_1981.none',
                    '/def_2018.none',
                    '/xyz_2018.none',
                    '/abc_2018.none',
                    '/def_1981.none'
                ])
                ),

            ('format IN ["DICOM", "NIFTI"]',
                set([
                    '/xyz_1899.nii',
                    '/xyz_2018.dcm',
                    '/bcd_1899.nii',
                    '/def_1899.nii',
                    '/abc_1981.nii',
                    '/abc_1899.nii',
                    '/bcd_2018.nii',
                    '/abc_2018.dcm',
                    '/bcd_1899.dcm',
                    '/def_1981.dcm',
                    '/abc_2018.nii',
                    '/abc_1981.dcm',
                    '/bcd_2018.dcm',
                    '/def_2018.nii',
                    '/def_2018.dcm',
                    '/xyz_1899.dcm',
                    '/abc_1899.dcm',
                    '/def_1899.dcm',
                    '/bcd_1981.nii',
                    '/xyz_1981.nii',
                    '/xyz_2018.nii',
                    '/xyz_1981.dcm',
                    '/def_1981.nii',
                    '/bcd_1981.dcm',
                ])
                ),

            ('(format == "NIFTI" OR NOT format == "DICOM") AND ("a" IN strings OR NOT "b" IN strings)',
                set([
                    '/abc_1899.none',
                    '/xyz_1899.mgz',
                    '/abc_1981.mgz',
                    '/abc_2018.nii',
                    '/xyz_1899.nii',
                    '/abc_1899.mgz',
                    '/def_1899.mgz',
                    '/def_1899.nii',
                    '/def_1899.none',
                    '/abc_1981.nii',
                    '/def_2018.nii',
                    '/xyz_2018.nii',
                    '/def_1981.nii',
                    '/abc_1899.nii',
                    '/xyz_1981.nii',
                    '/abc_2018.mgz',
                    '/def_1981.mgz',
                    '/xyz_2018.mgz',
                    '/xyz_1899.none',
                    '/def_2018.mgz',
                    '/xyz_1981.mgz',
                    '/xyz_1981.none',
                    '/abc_1981.none',
                    '/def_2018.none',
                    '/xyz_2018.none',
                    '/abc_2018.none',
                    '/def_1981.none'
                ])
                ),

            ('format > "DICOM"',
                set([
                    '/xyz_1899.nii',
                    '/xyz_1899.mgz',
                    '/bcd_2018.mgz',
                    '/bcd_1899.nii',
                    '/bcd_2018.nii',
                    '/def_1899.nii',
                    '/bcd_1981.mgz',
                    '/abc_1981.nii',
                    '/def_2018.mgz',
                    '/abc_1899.nii',
                    '/def_1899.mgz',
                    '/abc_2018.nii',
                    '/def_2018.nii',
                    '/abc_1981.mgz',
                    '/xyz_1981.mgz',
                    '/bcd_1981.nii',
                    '/xyz_1981.nii',
                    '/abc_2018.mgz',
                    '/xyz_2018.nii',
                    '/abc_1899.mgz',
                    '/def_1981.nii',
                    '/def_1981.mgz',
                    '/bcd_1899.mgz',
                    '/xyz_2018.mgz'
                ])
                ),

            ('format <= "DICOM"',
                set([
                    '/abc_1981.dcm',
                    '/def_1899.dcm',
                    '/abc_2018.dcm',
                    '/bcd_1899.dcm',
                    '/def_1981.dcm',
                    '/bcd_2018.dcm',
                    '/def_2018.dcm',
                    '/xyz_2018.dcm',
                    '/xyz_1899.dcm',
                    '/abc_1899.dcm',
                    '/xyz_1981.dcm',
                    '/bcd_1981.dcm',
                ])
                ),

            ('format > "DICOM" AND strings != ["b", "c", "d"]',
                set([
                    '/xyz_1899.nii',
                    '/xyz_1899.mgz',
                    '/abc_1981.mgz',
                    '/abc_2018.nii',
                    '/xyz_2018.nii',
                    '/abc_1899.mgz',
                    '/def_1899.mgz',
                    '/def_1899.nii',
                    '/abc_1981.nii',
                    '/def_2018.nii',
                    '/def_1981.nii',
                    '/abc_1899.nii',
                    '/xyz_1981.nii',
                    '/abc_2018.mgz',
                    '/def_1981.mgz',
                    '/xyz_2018.mgz',
                    '/def_2018.mgz',
                    '/xyz_1981.mgz'
                ])
                ),

            ('format <= "DICOM" AND strings == ["b", "c", "d"]',
                set([
                    '/bcd_2018.dcm',
                    '/bcd_1981.dcm',
                    '/bcd_1899.dcm',
                ])
                ),

            ('has_format in [false, null]',
                set([
                    '/def_1899.none',
                    '/abc_1899.none',
                    '/bcd_1899.none',
                    '/xyz_1899.none',
                    '/bcd_2018.none',
                    '/abc_1981.none',
                    '/def_2018.none',
                    '/xyz_2018.none',
                    '/abc_2018.none',
                    '/def_1981.none',
                    '/xyz_1981.none',
                    '/bcd_1981.none',
                ])
                ),

            ('format == null',
                set([
                    '/bcd_1981.none',
                    '/abc_1899.none',
                    '/def_1899.none',
                    '/bcd_2018.none',
                    '/abc_1981.none',
                    '/def_2018.none',
                    '/xyz_2018.none',
                    '/abc_2018.none',
                    '/def_1981.none',
                    '/bcd_1899.none',
                    '/xyz_1899.none',
                    '/xyz_1981.none'
                ])
                ),

            ('strings == null',
                set()),

            ('strings != NULL',
                set([
                    '/xyz_1899.nii',
                    '/xyz_2018.dcm',
                    '/xyz_1899.mgz',
                    '/bcd_2018.mgz',
                    '/bcd_1899.nii',
                    '/def_2018.none',
                    '/def_1899.mgz',
                    '/def_1899.nii',
                    '/bcd_1981.mgz',
                    '/abc_1981.nii',
                    '/def_2018.mgz',
                    '/abc_1899.nii',
                    '/bcd_2018.nii',
                    '/abc_2018.dcm',
                    '/xyz_1899.none',
                    '/bcd_1899.dcm',
                    '/bcd_1981.none',
                    '/def_1981.dcm',
                    '/abc_2018.nii',
                    '/def_1899.none',
                    '/xyz_1981.none',
                    '/abc_1981.dcm',
                    '/bcd_2018.dcm',
                    '/def_2018.nii',
                    '/abc_1981.mgz',
                    '/def_2018.dcm',
                    '/abc_1899.none',
                    '/xyz_1981.mgz',
                    '/xyz_1899.dcm',
                    '/abc_1899.dcm',
                    '/def_1899.dcm',
                    '/bcd_1981.nii',
                    '/def_1981.none',
                    '/xyz_1981.nii',
                    '/abc_2018.mgz',
                    '/xyz_2018.none',
                    '/xyz_2018.nii',
                    '/abc_1899.mgz',
                    '/bcd_1899.mgz',
                    '/bcd_2018.none',
                    '/abc_1981.none',
                    '/xyz_1981.dcm',
                    '/abc_2018.none',
                    '/def_1981.nii',
                    '/bcd_1981.dcm',
                    '/def_1981.mgz',
                    '/bcd_1899.none',
                    '/xyz_2018.mgz'
                ])
                ),

            ('format != NULL',
                set([
                    '/xyz_1899.nii',
                    '/xyz_1899.mgz',
                    '/bcd_2018.mgz',
                    '/bcd_1899.nii',
                    '/def_1899.mgz',
                    '/def_1899.nii',
                    '/bcd_1981.mgz',
                    '/abc_1981.nii',
                    '/def_2018.mgz',
                    '/abc_1899.nii',
                    '/bcd_2018.nii',
                    '/abc_2018.dcm',
                    '/xyz_1981.mgz',
                    '/def_1981.dcm',
                    '/abc_2018.nii',
                    '/abc_1981.dcm',
                    '/bcd_2018.dcm',
                    '/def_2018.nii',
                    '/bcd_1981.nii',
                    '/abc_1981.mgz',
                    '/def_2018.dcm',
                    '/bcd_1899.dcm',
                    '/xyz_2018.dcm',
                    '/xyz_1899.dcm',
                    '/abc_1899.dcm',
                    '/def_1899.dcm',
                    '/bcd_1899.mgz',
                    '/xyz_1981.nii',
                    '/abc_2018.mgz',
                    '/xyz_2018.nii',
                    '/abc_1899.mgz',
                    '/xyz_1981.dcm',
                    '/def_1981.nii',
                    '/bcd_1981.dcm',
                    '/def_1981.mgz',
                    '/xyz_2018.mgz'
                ])
                ),

            ('name like "%.nii"',
                set([
                    '/xyz_1899.nii',
                    '/xyz_2018.nii',
                    '/abc_2018.nii',
                    '/bcd_1899.nii',
                    '/bcd_2018.nii',
                    '/def_1899.nii',
                    '/abc_1981.nii',
                    '/def_2018.nii',
                    '/def_1981.nii',
                    '/bcd_1981.nii',
                    '/abc_1899.nii',
                    '/xyz_1981.nii'
                ])
                ),

            ('name ilike "%A%"',
                set([
                    '/abc_1899.none',
                    '/abc_1899.nii',
                    '/abc_2018.nii',
                    '/abc_1899.mgz',
                    '/abc_1899.dcm',
                    '/abc_1981.dcm',
                    '/abc_1981.nii',
                    '/abc_1981.mgz',
                    '/abc_2018.mgz',
                    '/abc_2018.dcm',
                    '/abc_2018.none',
                    '/abc_1981.none'
                ])
                ),

            ('all',
                set([
                    '/xyz_1899.nii',
                    '/xyz_2018.dcm',
                    '/xyz_1899.mgz',
                    '/bcd_2018.mgz',
                    '/bcd_1899.nii',
                    '/def_2018.none',
                    '/def_1899.mgz',
                    '/def_1899.nii',
                    '/bcd_1981.mgz',
                    '/abc_1981.nii',
                    '/def_2018.mgz',
                    '/abc_1899.nii',
                    '/bcd_2018.nii',
                    '/abc_2018.dcm',
                    '/xyz_1899.none',
                    '/bcd_1899.dcm',
                    '/bcd_1981.none',
                    '/def_1981.dcm',
                    '/abc_2018.nii',
                    '/def_1899.none',
                    '/xyz_1981.none',
                    '/abc_1981.dcm',
                    '/bcd_2018.dcm',
                    '/def_2018.nii',
                    '/abc_1981.mgz',
                    '/def_2018.dcm',
                    '/abc_1899.none',
                    '/xyz_1981.mgz',
                    '/xyz_1899.dcm',
                    '/abc_1899.dcm',
                    '/def_1899.dcm',
                    '/bcd_1981.nii',
                    '/def_1981.none',
                    '/xyz_1981.nii',
                    '/abc_2018.mgz',
                    '/xyz_2018.none',
                    '/xyz_2018.nii',
                    '/abc_1899.mgz',
                    '/bcd_1899.mgz',
                    '/bcd_2018.none',
                    '/abc_1981.none',
                    '/xyz_1981.dcm',
                    '/abc_2018.none',
                    '/def_1981.nii',
                    '/bcd_1981.dcm',
                    '/def_1981.mgz',
                    '/bcd_1899.none',
                    '/xyz_2018.mgz'
                ])
                )):
        for tested_filter in (filter, f'{filter} AND ALL', f'ALL AND {filter}'):
            print('!!!', tested_filter)
            try:
                documents = set(document['name'] for document in files.filter(tested_filter))
                assert documents == expected, f'{documents} != {expected}'
            except Exception as e:
                e.message = f'While testing filter : {tested_filter}\n{e}'
                e.args = (e.message,)
                raise
        all_documents = set(document['name'] for document in files.filter('ALL'))
        for tested_filter in (f'({filter}) OR ALL', f'ALL OR ({filter})'):
            try:
                documents = set(document['name'] for document in files.filter(tested_filter))
                assert documents == all_documents, f'{documents} != {all_documents}'
            except Exception as e:
                e.message = 'While testing filter : %s\n%s' % (str(tested_filter), str(e))
                e.args = (e.message,)
                raise



with sqlite3.connect(':memory:') as db:
    db.execute('create table study ('
      'study_id text not null primary key,'
      'label text,'
      '_json text)')
    db.execute('create table subject ('
        'study_id text not null,'
        'subject_id text not null,'
        '_json text,'
        'primary key (study_id, subject_id))')


    dictsqlite = Dictsqlite(db)
    studies = dictsqlite['study']
    studies['usad'] = {
        'label': 'Ultimate Study on Alzheimer Disease',
        'participants': 10000,
        'recruitement': 'healthy subjects having more than 65 years old'
    }
    subjects = dictsqlite['subject']
    subjects['usad', 'usad_001'] = {
        'birth_date': '1935-03-03'
    }

    studies.add({
        'study_id': 'gspd',
        'label': 'Greatest Study on Parkinson Disease',
        'participants': 30000,
        'recruitement': 'healthy subjects'
    })
    subjects = dictsqlite['subject']
    subjects.add({
        'study_id': 'gspd',
        'subject_id': 'gspd_001',
        'birth_date': '1935-03-03'
    })

    print('usad:', studies['usad'])
    print('usad_001:', subjects['usad', 'usad_001'])
    print('number of studies:', studies.count())
    for study in studies:
        print(study)
    
    test_filters(dictsqlite)
