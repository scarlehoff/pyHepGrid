import pyHepGrid.src.header
"""Routines to be used by main.py"""

def management_routine(backend, args):

    if args.yes:
        backend.dont_ask_dont_tell()

    if args.list_disabled:
        backend.set_list_disabled()

    if args.get_data and pyHepGrid.src.header.finalisation_script:
        backend.get_data(0, custom_get=pyHepGrid.src.header.finalisation_script)
        exit(0)

    if args.updateArc:
        if not args.runArc:
            pyHepGrid.src.header.logger.critical("Update ARC can only be used with ARC")
        backend.update_stdout()
        exit(0)

    if args.idjob:
        id_str = args.idjob
    else:
        backend.list_runs(args.find)
        id_str = input("> Select id(s): ")

    id_list_raw = str(id_str).split(",")
    id_list = []
    active_ids = backend.get_active_dbids()
    if len(id_list_raw) == 1 and (id_list_raw[0].lower() == "all"
                                  or id_list_raw[0] == "-"
                                  or id_list_raw[0] == ":"):
        id_list_raw = active_ids

    # Expand out id ranges using delimiters
    for id_selected in id_list_raw:
        added = False
        for range_delimiter in ["-",":"]:
            if range_delimiter in id_selected:
                added = True
                id_limits = id_selected.split(range_delimiter)
                # Extend unspecified ranges (e.g. "10-" -> 10-end)
                if id_limits[0] == "":
                    id_limits[0] = min(int(id_limits[1]),int(active_ids[0]))
                if id_limits[1] == "":
                    id_limits[1] = max(int(id_limits[0]),int(active_ids[-1]))
                for id_int in range(int(id_limits[0]), int(id_limits[1]) + 1):
                    id_list.append(str(id_int))
        if not added:
            id_list.append(id_selected)
    no_ids = len(id_list)

    if args.get_grid_stdout:
        # if no_ids > 1:
        #     import pyHepGrid.src.header as header
        #     header.logger.critical("Only one job at a time can be used when getting grid output from stdout in case to prevent overwriting")
        if not args.runArc:
            pyHepGrid.src.header.logger.critical("Getting grid output from stdout only a valid mode for Arc warmups")

    for idx, db_id in enumerate(id_list):
        # Setup for printing/function args
        jdx= idx+1
        request_fields = ["runcard", "jobtype","runfolder","iseed","no_runs"]
        alljobinfo = backend.dbase.list_data(backend.table, request_fields, db_id)
        # raise Exception
        if len(alljobinfo)==0:
            pyHepGrid.src.header.logger.critical("Job {0} requested, which does not exist in database".format(db_id))
        jobinfo = alljobinfo[0]
        jobname = "{0} ({1})".format(jobinfo["runcard"],jobinfo["jobtype"])
        jobid = backend.get_id(db_id) # a list
        printstr = "{0} for job"+" {0}: {3:20} [{1}/{2}]".format(db_id,jdx,no_ids,jobname)

        if args.simple_string:
            backend.set_oneliner_output()

        # Could we make this more generic? i.e pass function with opt args using a dictionary
        # rather than just making copies for every possibility
        # Options that keep the database entry after they are done
        if args.filter_jobs_by_status is not None:
            pyHepGrid.src.header.logger.warn("Applying job status filter. Please ensure you have run stats directly before this command to update job statuses.")
            pyHepGrid.src.header.logger.info("Job status filter: {0}".format(" ".join(args.filter_jobs_by_status)))
            try:
                status_codes = [getattr(backend, "c"+i.upper() ) for i in args.filter_jobs_by_status]
            except AttributeError as e:
                pyHepGrid.src.header.logger.critical("Invalid job status given: {0}".format(str(e).split(" ")[-1][2:-1]))
            statuses = backend._get_old_status(db_id)
            new_joblist = []
            for jid, status in zip(jobid, statuses):
                if status in status_codes:
                    new_joblist.append(jid)
            jobid = new_joblist

        if args.stats:
            backend.stats_job(db_id)
        if args.info or args.infoVerbose:
            pyHepGrid.src.header.logger.info(printstr.format("Retrieving information"))
            backend.status_job(jobid, args.infoVerbose)
        if args.renewArc:
            pyHepGrid.src.header.logger.info(printstr.format("Renewing proxy"))
            backend.renew_proxy(jobid)
        if args.printme:
            pyHepGrid.src.header.logger.info(printstr.format("Printing information"))
            backend.cat_job(jobid, jobinfo, print_stderr = args.error)
            pyHepGrid.src.header.logger.info("\n") # As our % complete sometimes has a carriage return :P
        if args.printmelog:
            pyHepGrid.src.header.logger.info(printstr.format("Printing information from logfile"))
            backend.cat_log_job(jobid, jobinfo)
        if args.checkwarmup:
            backend.check_warmup_files(db_id, args.runcard, resubmit=args.resubmit)
        if args.getmewarmup:
            pyHepGrid.src.header.logger.info(printstr.format("Retrieving warmup"))
            backend.bring_current_warmup(db_id)
        if args.get_grid_stdout:
            backend.get_grid_from_stdout(jobid, jobinfo)
        if args.completion:
            backend.get_completion_stats(jobid, jobinfo, args)

        # Options that deactivate the database entry once they're done
        if args.get_data:
            pyHepGrid.src.header.logger.info(printstr.format("Retrieving data"))
            backend.get_data(db_id)
            if not args.done and not args.runSlurmProduction: # if --done is used we assume there are jobs which are _not_ done
                backend.disable_db_entry(db_id)
        if args.kill_job:
            pyHepGrid.src.header.logger.info(printstr.format("Killing"))
            backend.kill_job(jobid, jobinfo)
            backend.disable_db_entry(db_id)
        if args.clean:
            pyHepGrid.src.header.logger.info(printstr.format("Cleaning"))
            backend.clean_job(jobid)
            backend.disable_db_entry(db_id)

        # Enable back any database entry
        if args.enableme:
            backend.enable_db_entry(db_id)
        if args.disableme:
            backend.disable_db_entry(db_id)

        if not any([args.stats, args.info, args.infoVerbose, args.renewArc, args.printme,
                    args.printmelog, args.checkwarmup, args.getmewarmup, args.get_grid_stdout,
                    args.completion, args.get_data, args.kill_job, args.clean, args.enableme,
                    args.disableme]):
            pyHepGrid.src.header.logger.plain(" ".join(i for i in jobid))
