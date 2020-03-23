class Tests:
    # Defining table names as dictionary keys and the number of records in a table as dictionary value
    # Expected record count for a table post successful loading of data into the tables
    tests_results = {'public.staging_events':8056, 'public.staging_songs':29, 'public.songs':29, 'public.users':104, 'public.time':6820, 'public.artists':29}