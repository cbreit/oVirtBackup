import logging
import sys
import time

import ovirtsdk4 as sdk
import ovirtsdk4.types as otypes

from backup import arguments_to_dict, create_argparser
from config import Config


# --------------------------------------------------------------------------------------------------

class OvirtBackup:
    def __init__(self, argv, url=None, username=None, password=None, ca_file=None):

        self.__config = config = OvirtBackup.load_config(argv)
        OvirtBackup.initialize_logger_config(config.get_logger_format(),
                                             config.get_logger_file_path(), config.get_debug())

        self.__logger = logging.getLogger()

        if url is None:
            url = config.get_server()

        if username is None:
            username = config.get_username()

        if password is None:
            password = config.get_password()

        if ca_file is None:
            ca_file = config.get_ca()


        self.__connection = OvirtBackup.connect_to_server(url, username, password, ca_file, self.__logger)
        self.__vms_service = self.__connection.system_service().vms_service()
        self.__time_start = int(time.time())

        OvirtBackup.test_config(self.__config, self.__logger, self.__connection)

    # --------------------------------------------------------------------------------------------------

    def __del__(self):
        self.__connection.close()

    # --------------------------------------------------------------------------------------------------

    def backup_vm(self, vm_name, vm_clone_name, export_name):
        vms_service = self.__vms_service
        connection = self.__connection
        config = self.__config

        vm = vms_service.list(search='name=' + vm_name)[0]
        if vm is None:
            self.__logger.error("Following VM does not exist anymore: " + vm_name)
            raise(ValueError("Following VM does not exist anymore: " + vm_name))

        snapshots_service = vms_service.vm_service(vm.id).snapshots_service()

        snaps = snapshots_service.list()
        for snap in snaps:
            if snap.description.find(config.get_snapshot_description()) != -1:
                OvirtBackup.remove_snapshot(snapshots_service, snap, self.__logger)

        OvirtBackup.check_free_space(connection, config, vms_service, vm)

        snapshot = OvirtBackup.create_snapshot(snapshots_service, config.get_snapshot_description(), False, self.__logger)
        vm = OvirtBackup.clone_vm_from_snapshot(vms_service, snapshot, vm_clone_name,
                                                config.get_cluster_name(), self.__logger)
        OvirtBackup.remove_snapshot(snapshots_service, snapshot, self.__logger)

        OvirtBackup.export_vm(vms_service, vm, export_name, self.__logger)
        OvirtBackup.remove_vm(vms_service, vm, self.__logger)

        OvirtBackup.remove_old_backups(self.__connection, self.__config, self.__logger, vm_name)

        time_end = int(time.time())
        time_diff = (time_end - self.__time_start)
        time_minutes = int(time_diff / 60)
        time_seconds = time_diff % 60

        self.__logger.info("Duration: %s:%s minutes", time_minutes, time_seconds)
        self.__logger.info("VM exported as %s", vm_clone_name)
        self.__logger.info("Backup done for: %s", vm.name)

    # --------------------------------------------------------------------------------------------------

    def run_backup(self):
        config = self.__config
        failed_vms = []
        for vm_from_list in config.get_vm_names():
            config.clear_vm_suffix()
            vm_clone_name = vm_from_list + config.get_vm_middle() + config.get_vm_suffix()

            # TODO: Extend logging (and maybe email)
            try:
                OvirtBackup.check_name_length(vm_clone_name, config, self.__logger)
                self.__logger.info("##################  Creating backup of VM: %s", vm_from_list)
                self.__logger.info("##################  VM Backup Name: %s", vm_clone_name)
                self.backup_vm(vm_from_list, vm_clone_name, config.get_export_domain())
            except Exception as ex:
                failed_vms.append(vm_from_list)
                self.__logger.error("Error creating backup of: " + vm_clone_name + " -- " + str(ex))

        return failed_vms

    # --------------------------------------------------------------------------------------------------
    @staticmethod
    def load_config(argv):
        p = create_argparser()
        opts = p.parse_args(argv)
        config_arguments = arguments_to_dict(opts)

        with opts.config_file:
            config = Config(opts.config_file, opts.debug, config_arguments)

        return config

    # --------------------------------------------------------------------------------------------------
    @staticmethod
    def initialize_logger_config(logger_fmt, logger_file_path, debug):
        logger_options = {
            "format": logger_fmt,
            "level":  logging.DEBUG if debug else logging.INFO,
        }
        if logger_file_path:
            logger_options['filename'] = logger_file_path
        logging.basicConfig(**logger_options)

    # --------------------------------------------------------------------------------------------------
    @staticmethod
    def test_config(config, logger, connection):

        sd_service = connection.system_service().storage_domains_service()
        vms_service = connection.system_service().vms_service()
        clusters_service = connection.system_service().clusters_service()

        # Test if config export_domain and storage_domain are valid

        if sd_service.list(search=config.get_export_domain())[0] is None:
            logger.error("!!! Check the export_domain in the config")
            raise(AttributeError(config.get_export_domain() + ' not found!'))

        if sd_service.list(search=config.get_storage_domain())[0] is None:
            logger.error("!!! Check the storage_domain in the config")
            raise(AttributeError(config.get_storage_domain() + ' not found!'))

        # Test if config cluster_name is valid
        if clusters_service.list(search=config.get_cluster_name())[0] is None:
            logger.error("!!! Check the cluster_name in the config")
            raise(AttributeError(config.get_cluster_name() + ' not found!'))

        # Test if all VM names are valid
        vms_existing = [x.name for x in vms_service.list()]

        for vm in config.get_vm_names():
            if vm not in vms_existing:
                logger.error("!!! There are no VM with the following name in your cluster: %s", vm)
                raise (AttributeError("VM does not exist: " + vm))

        # Test if config vm_middle is valid
        if not config.get_vm_middle():
            logger.error("!!! It's not valid to leave vm_middle empty")
            raise(AttributeError("It's not valid to leave vm_middle empty"))

    # --------------------------------------------------------------------------------------------------
    @staticmethod
    def check_name_length(vm_name, config, logger):
        length = len(vm_name)
        if length > config.get_vm_name_max_length():
            logger.error("!!! VM name with middle and suffix are to long (size: %s, allowed %s) !!!", length,
                         config.get_vm_name_max_length())
            logger.info("VM name: %s", vm_name)

    # --------------------------------------------------------------------------------------------------
    @staticmethod
    def check_free_space(connection, config, vms_service, vm):
        sd_service = connection.system_service().storage_domains_service()
        sd = sd_service.list(search=config.get_storage_domain())[0]
        vm_service = vms_service.vm_service(vm.id)
        disk_attachments_service = vm_service.disk_attachments_service()

        vm_size = 0
        for disk_attachment in disk_attachments_service.list():
            disk = connection.follow_link(disk_attachment.disk)
            # For safety reason "vm.actual_size" is not used
            if disk.provisioned_size is not None:
                vm_size += disk.provisioned_size
        storage_space_threshold = 0
        if config.get_storage_space_threshold() > 0:
            storage_space_threshold = config.get_storage_space_threshold()
        vm_size *= (1 + storage_space_threshold)
        if (sd.available - vm_size) <= 0:
            raise Exception(
                "!!! The is not enough free storage on the storage domain '%s' available to backup the VM '%s'" % (
                    config.get_storage_domain(), vm_service.name))

    # --------------------------------------------------------------------------------------------------
    @staticmethod
    def remove_old_backups(connection, config, logger, vm_name):
        import datetime, re

        export_name = config.get_export_domain()
        keep_count = config.get_backup_keep_count()
        keep_count_by_number = config.get_backup_keep_count_by_number()

        sd_service = connection.system_service().storage_domains_service()
        ed = sd_service.list(search=export_name)[0]

        vm_search_regexp = ("%s%s*" % (vm_name, config.get_vm_middle()))
        export_vms_service = sd_service.storage_domain_service(ed.id).vms_service()
        exported_vms = export_vms_service.list()

        target_vms = []
        for vm in exported_vms:
            if re.search(vm_search_regexp, vm.name):
                target_vms.append(vm)

        target_vms.sort(key=lambda x: x.creation_time)
        if keep_count:
            for vm in target_vms:
                datetime_start = datetime.datetime.combine(
                    (datetime.date.today() - datetime.timedelta(keep_count)), datetime.datetime.min.time())
                timestamp_start = time.mktime(datetime_start.timetuple())
                datetime_creation = vm.creation_time.replace(hour=0, minute=0, second=0)
                timestamp_creation = time.mktime(datetime_creation.timetuple())

                if timestamp_creation < timestamp_start:
                    logger.info("Backup deletion started for backup: %s", vm.name)
                    OvirtBackup.remove_exported_vm(export_vms_service, vm, logger)

        if keep_count_by_number:
            while len(target_vms) > keep_count_by_number:
                vm = target_vms.pop(0)
                logger.info("Backup deletion started for backup: %s", vm.name)
                OvirtBackup.remove_exported_vm(export_vms_service, vm, logger)

    # --------------------------------------------------------------------------------------------------
    @staticmethod
    def wait(
        service,
        condition,
        logger,
        fail_condition=lambda e: False,
        timeout=600,
        wait=True,
        poll_interval=10
    ):
        """
        Wait until entity fulfill expected condition.
        :param service: service of the entity
        :param condition: condition to be fulfilled
        :param logger: python logger
        :param fail_condition: if this condition is true, raise Exception
        :param timeout: max time to wait in seconds
        :param wait: if True wait for condition, if False don't wait
        :param poll_interval: Number of seconds we should wait until next condition check
        """
        # Wait until the desired state of the entity:
        if wait:
            start = time.time()
            while time.time() < start + timeout:

                entity = OvirtBackup.get_entity(service)
                if condition(entity):
                    return
                elif fail_condition(entity):
                    logger.error("Error while waiting on result state of the entity.")
                    raise Exception("Error while waiting on result state of the entity.")

                time.sleep(float(poll_interval))

            logger.error("Timeout exceed while waiting on result state of the entity.")
            raise Exception("Timeout exceed while waiting on result state of the entity.")

    # --------------------------------------------------------------------------------------------------
    @staticmethod
    def get_entity(service, get_params=None):
        entity = None
        try:
            if get_params is not None:
                entity = service.get(**get_params)
            else:
                entity = service.get()
        except sdk.Error:
            # We can get here 404, we should ignore it, in case
            # of removing entity for example.
            pass

        return entity

    # --------------------------------------------------------------------------------------------------
    @staticmethod
    def remove_snapshot(snapshots_service, snapshot, logger):
        logger.info('Removing snapshot ' + snapshot.description + '...')
        if snapshot:
            snapshot_service = snapshots_service.snapshot_service(snapshot.id)
            snapshot_service.remove()
            OvirtBackup.wait(
                service=snapshot_service,
                condition=lambda snap: snap is None,
                logger=logger,
                wait=True,
                timeout=180000,
            )
            logger.info('  --> done')
            return snapshot.id if snapshot else None

    # --------------------------------------------------------------------------------------------------
    @staticmethod
    def create_snapshot(snapshots_service, name, use_memory, logger):
        logger.info('Creating snapshot...')
        snapshot = snapshots_service.add(
            otypes.Snapshot(
                description=name,
                persist_memorystate=use_memory
            )
        )
        OvirtBackup.wait(
            service=snapshots_service.snapshot_service(snapshot.id),
            condition=lambda snap: snap.snapshot_status == otypes.SnapshotStatus.OK,
            logger=logger,
        )
        logger.info('  --> done')
        return snapshot

    # --------------------------------------------------------------------------------------------------
    @staticmethod
    def clone_vm_from_snapshot(vms_service, snapshot, vm_name, cluster_name, logger):
        logger.info('Cloning VM from snapshot...')
        cloned_vm = vms_service.add(
            vm=otypes.Vm(
                name=vm_name,
                snapshots=[
                    otypes.Snapshot(
                        id=snapshot.id
                    )
                ],
                cluster=otypes.Cluster(
                    name=cluster_name
                )
            )
        )

        OvirtBackup.wait(
            service=vms_service.vm_service(cloned_vm.id),
            condition=lambda vm: vm.status == otypes.VmStatus.DOWN,
            logger=logger,
            wait=True,
            timeout=180000
        )
        logger.info('  --> done')
        return cloned_vm

    # --------------------------------------------------------------------------------------------------
    @staticmethod
    def export_vm(vms_service, vm, export_name, logger):
        logger.info('Exporting VM...')
        vm_service = vms_service.vm_service(vm.id)
        vm_service.export(
            exclusive=True,
            discard_snapshots=True,
            storage_domain=otypes.StorageDomain(
                name=export_name
            )
        )
        OvirtBackup.wait(
            service=vms_service.vm_service(vm.id),
            condition=lambda sv: sv.status == otypes.VmStatus.DOWN,
            logger=logger,
        )
        logger.info('  --> done')

    # --------------------------------------------------------------------------------------------------
    @staticmethod
    def remove_exported_vm(export_vms_service, vm, logger):
        vm_service = export_vms_service.vm_service(vm.id)

        if vm_service:
            logger.info('Removing exported VM...')
            vm_service.remove()
        else:
            logger.info('  Exported VM to remove does not exist anymore...')
            return 0

        OvirtBackup.wait(
            service=vm_service,
            condition=lambda sv: sv is None,
            logger=logger,
        )
        logger.info('  --> done')

    # --------------------------------------------------------------------------------------------------
    @staticmethod
    def remove_vm(vms_service, vm, logger):
        logger.info('Removing VM...')
        if vm:
            vm_service = vms_service.vm_service(vm.id)
            vm_service.remove()
            OvirtBackup.wait(
                service=vm_service,
                condition=lambda sv: sv is None,
                logger=logger,
            )
            logger.info('  --> done')

    # --------------------------------------------------------------------------------------------------
    @staticmethod
    def connect_to_server(url, username, password, ca_file, logger, debug=False):
        connection = sdk.Connection(
            url=url,
            username=username,
            password=password,
            ca_file=ca_file,
            debug=debug,
            log=logger,
            timeout=0
        )
        return connection

# --------------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------

def main(argv):
    ovirt_backup = OvirtBackup(argv)
    ovirt_backup.run_backup()


if __name__ == "__main__":
    if sys.version_info[0] < 3:
        raise Exception("Python 3 or a more recent version is required.")

    main(sys.argv[1:])
