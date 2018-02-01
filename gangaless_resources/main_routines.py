"""Routines to be used by main.py"""

def management_routine(backend, args):
    if args.yes:
        backend.dont_ask_dont_tell()
    if args.list_disabled:
        backend.set_list_disabled()

    from header import finalisation_script
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
        id_str = input("> Select id to act upon: ")

    id_list_raw = str(id_str).split(",")
    id_list = []
    if len(id_list_raw) == 1 and id_list_raw[0].lower() == "all":
        id_list_raw = backend.get_active_dbids()
    for id_selected in id_list_raw:
        if "-" in id_selected:
            id_limits = id_selected.split("-")
            for id_int in range(int(id_limits[0]), int(id_limits[1]) + 1):
                id_list.append(str(id_int))
        else:
            id_list.append(id_selected)

    for db_id in id_list:
        jobid = backend.get_id(db_id) # a list
        # Options that keep the database entry after they are done
        if args.stats:
            backend.stats_job(db_id)
        elif args.statsCheat:
            backend.stats_job_cheat(db_id)
        elif args.info or args.infoVerbose:
            print("Retrieving information . . . ")
            backend.status_job(jobid, args.infoVerbose)
        elif args.renewArc:
            print("Renewing proxy for the job . . . ")
            backend.renew_proxy(jobid)
        elif args.printme:
            print("Printing information . . . ")
            backend.cat_job(jobid, print_stderr = args.error)
        elif args.printmelog:
            print("Printing information . . . ")
            backend.cat_log_job(jobid)
        elif args.getmewarmup:
            print("Retrieving warmup")
            backend.bring_current_warmup(db_id)

        # Options that deactivate the database entry once they're done
        elif args.get_data:
            print("Retrieving job data")
            backend.get_data(db_id)
            if not args.done: # if --done is used we assume there are jobs which are _not_ done
                backend.disable_db_entry(db_id)
        elif args.kill_job:
            print("Killing the job")
            backend.kill_job(jobid)
            backend.disable_db_entry(db_id)
        elif args.clean:
            print("Cleaning job . . . ")
            backend.clean_job(jobid)
            backend.disable_db_entry(db_id)
        # Enable back any database entry
        elif args.enableme:
            backend.enable_db_entry(db_id)
        elif args.disableme:
            backend.disable_db_entry(db_id)
        else:
            print(jobid)


