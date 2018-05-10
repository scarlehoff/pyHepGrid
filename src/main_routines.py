"""Routines to be used by main.py"""

def management_routine(backend, args):

    if args.yes:
        backend.dont_ask_dont_tell()
    if args.list_disabled:
        backend.set_list_disabled()

    from src.header import finalisation_script
    if args.get_data and finalisation_script:
        backend.get_data(0, custom_get = finalisation_script)
        exit(0)

    if args.updateArc:
        if not args.runArc: 
            raise Exception("Update ARC can only be used with ARC")
        backend.update_stdout()
        exit(0)
    if args.idjob:
        id_str = args.idjob
    else:
        backend.list_runs(args.find)
        id_str = input("> Select id(s): ")

    id_list_raw = str(id_str).split(",")
    id_list = []
    if len(id_list_raw) == 1 and id_list_raw[0].lower() == "all":
        id_list_raw = backend.get_active_dbids()

    # Expand out id ranges using delimiters
    for id_selected in id_list_raw:
        added = False
        for range_delimiter in ["-",":"]:
            if range_delimiter in id_selected:
                added = True
                id_limits = id_selected.split(range_delimiter)
                for id_int in range(int(id_limits[0]), int(id_limits[1]) + 1):
                    id_list.append(str(id_int))
        if not added:
            id_list.append(id_selected)

    no_ids = len(id_list)

    for idx,db_id in enumerate(id_list):
        # Setup for printing/function args
        jdx= idx+1
        jobinfo = backend.dbase.list_data(backend.table, ["runcard", "jobtype","runfolder","iseed","no_runs"], db_id)[0]
        jobname = "{0} ({1})".format(jobinfo["runcard"],jobinfo["jobtype"])
        jobid = backend.get_id(db_id) # a list
        printstr = "{0} for job"+" {0}: {3:20} [{1}/{2}]".format(db_id,jdx,no_ids,jobname)


        # Could we make this more generic? i.e pass function with opt args using a dictionary
        # rather than just making copies for every possibility
        # Options that keep the database entry after they are done
        if args.simple_string:
            backend.set_oneliner_output()

        if args.stats:
            backend.stats_job(db_id)
        elif args.statsCheat:
            backend.stats_job_cheat(db_id)
        elif args.info or args.infoVerbose:
            print(printstr.format("Retrieving information"))
            backend.status_job(jobid, args.infoVerbose)
        elif args.renewArc:
            print(printstr.format("Renewing proxy"))
            backend.renew_proxy(jobid)
        elif args.printme:
            print(printstr.format("Printing information"))
            backend.cat_job(jobid, jobinfo, print_stderr = args.error)
            print("\n") # As our % complete sometimes has a carriage return :P
        elif args.printmelog:
            print(printstr.format("Printing information from logfile"))
            backend.cat_log_job(jobid)
        elif args.getmewarmup:
            print(printstr.format("Retrieving warmup"))
            backend.bring_current_warmup(db_id)
        elif args.checkwarmup:
            backend.check_warmup_files(db_id, args.runcard, resubmit=args.resubmit)


        # Options that deactivate the database entry once they're done
        elif args.get_data:
            print(printstr.format("Retrieving data"))
            backend.get_data(db_id)
            if not args.done: # if --done is used we assume there are jobs which are _not_ done
                backend.disable_db_entry(db_id)
        elif args.kill_job:
            print(printstr.format("Killing"))
            backend.kill_job(jobid)
            backend.disable_db_entry(db_id)
        elif args.clean:
            print(printstr.format("Cleaning"))
            backend.clean_job(jobid)
            backend.disable_db_entry(db_id)

        # Enable back any database entry
        elif args.enableme:
            backend.enable_db_entry(db_id)
        elif args.disableme:
            backend.disable_db_entry(db_id)
        else:
            print(" ".join(i for i in jobid))


