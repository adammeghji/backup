# encoding: utf-8

##
# Neo4j Database Backup
#
# This module allows for consistent snapshots of Neo4j Graph DB data files,
# for those deployments which do are not using the Neo4J HA or incremental
# backup solutions.
#
# This technique uses LVM2 disks to take a point-in-time snapshot of your disks
# to a temporary mount point, then creates a tarball, and applies the compressor
#
# Optionally, you can specify a "lock_url" which temporarily disconnects your
# Graph DB, and specify a "unlock_url" which brings it back up.  Stopping your
# DB while the snapshot is being created is how we achieve consistency.
# Snapshot creation is almost instantaneous, so any downtime is mitigated.
#

##
# REQUIREMENTS:
# - LVM2 tools (apt-get install lvm2)
# - "vg_name" must be a valid LVM Volume Group
# - "lv_name" must be a valid LVM Logical Volume on "vg_name" group
# - "db_dir" must be a mounted on the "lv_name" volume and contains  Neo4j data

##
# SETUP:
# Your Neo4j data must be mounted on an LVM Logical Volume, via an LVM Volume
# Group with enough free extents to store the snapshot size.  It is recommended
# that you create the VG with a "free extents" setting of 50%, so the entire
# disk can always be snapshotted.
#

##
# CONFIGURATION:
#
# Your backup model configuration should look like this:
#
#  database Neo4j do |db|
#    db.name       = "production"
#    db.vg_name    = "/dev/uniiverse-ubuntu-server"
#    db.lv_name    = "/dev/uniiverse-ubuntu-server/galileo"
#    db.db_dir     = "/data/galileo/db/production"
#    db.sudo       = true
#    db.lock_url   = "http://localhost:4242/system/shutdown"
#    db.unlock_url = "http://localhost:4242/system/startup"
#  end
#

module Backup
  module Database
    class Neo4j < Base

      ##
      # Name is the name of the backup
      attr_accessor :name

      ##
      # VG Name of the LVM2 volume group, i.e. "VG Name" in 'vgdisplay'
      # e.g. /dev/uniiverse-ubuntu-server
      attr_accessor :vg_name

      ##
      # LV Name of the LVM2 logical volume, i.e. "LV Name" in 'lvdisplay'
      # e.g. /dev/uniiverse-ubuntu-server/neo4j
      attr_accessor :lv_name

      ##
      # DB Directory where your Neo4j data resides, i.e.
      # e.g. /data/neo4j/db/production"
      attr_accessor :db_dir

      ##
      # (optional) Run commands as root via sudo (required for LVM, etc)
      attr_accessor :sudo

      ##
      # (optional) URL which must return 200 after database has been shutdown
      attr_accessor :lock_url

      ##
      # (optional) URL which must return 200 after database has been started
      attr_accessor :unlock_url

      ##
      # Creates a new instance of the Neo4j adapter object
      def initialize(model, database_id = nil, &block)
        super(model, database_id)

        instance_eval(&block) if block_given?

        @sudo = "sudo" if !!@sudo
        @overhead_mb ||= 10 # add extra 10mb buffer

        utility(:lvcreate)
        utility(:lvremove)
      end

      def perform!
        super

        lock_database if @lock_url

        begin
          mount_snapshot

          unlock_database if @unlock_url

          package!

        ensure
          unmount_snapshot
        end
      end

      private

      def lock_database
        Logger.message(
          "#{ @name } started locking DB at:\n" +
          "  '#{ @lock_url }'"
        )
        get @lock_url
      end

      def unlock_database
        Logger.message(
          "#{ @name } started unlocking DB at:\n" +
          "  '#{ @unlock_url }'"
        )
        get @unlock_url
      end

      def get(raw_uri)
        uri = URI.parse(raw_uri)
        resp = Net::HTTP.get_response(uri)
        raise "ERROR: Could not GET: #{resp}\n" unless resp.code.to_i == 200
      end

      # temporary mount point for the snapshot device
      def mount_path
        "#{@dump_path}/#{@name}"
      end

      def snapshot_device
        "#{@vg_name}/#{@name}_snapshot"
      end

      def snapshot_size
        run("#{@sudo} du #{@db_dir} -m -s").split("\t")[0].to_i + @overhead_mb
      end

      def mount_snapshot
        unmount_snapshot # refresh

        pipeline = Pipeline.new
        pipeline << "#{@sudo} lvcreate -L#{snapshot_size} -s -n #{snapshot_device} #{@lv_name} && " +
                    "#{@sudo} mkdir -p #{mount_path} && " +
                    "#{@sudo} mount #{snapshot_device} #{mount_path}"

        pipeline.run

        if pipeline.success?
          Logger.message(
            "#{ @name } created snapshot at:\n" +
            "  '#{ mount_path }'"
          )
        else
          raise Errors::Database::PipelineError,
            "#{ @name } Failed to create snapshot at:\n" +
            "'#{ mount_path }'\n" +
            pipeline.error_messages
        end
      end

      def unmount_snapshot
        pipeline = Pipeline.new

        if File.exists? mount_path
          pipeline << "#{@sudo} umount #{mount_path} 2>&1 >/dev/null && " +
                      "#{@sudo} rmdir #{mount_path} 2>&1 >/dev/null"
        end

        pipeline << "(#{@sudo} lvremove #{snapshot_device} -f 2> /dev/null; true)"
        pipeline.run

        if pipeline.success?
          Logger.message(
            "#{ @name } removed snapshot at:\n" +
            "  '#{ mount_path }'"
          )
        else
          raise Errors::Database::PipelineError,
            "#{ @name } Failed to remove snapshot at:\n" +
            "'#{ mount_path }'\n" +
            pipeline.error_messages
        end
      end

      def package!
        base_dir  = File.dirname(mount_path)
        data_dir  = File.basename(mount_path)
        dump_dir  = File.basename(@dump_path)
        timestamp = Time.now.strftime('%Y-%m-%d_%H-%M')
        outfile   = @dump_path + '-' + timestamp + '.tar'

        pipeline = Pipeline.new
        pipeline << "#{@sudo} #{ utility(:tar) } -cPf - -C '#{ base_dir }' '#{ data_dir }'"

        if @model.compressor
          @model.compressor.compress_with do |command, ext|
            pipeline << command
            outfile << ext
          end
        end

        pipeline << "cat > #{ outfile }"

        pipeline.run
        if pipeline.success?
          Logger.message(
            "#{ @name } completed compressing and packaging:\n" +
            "  '#{ outfile }'"
          )
          FileUtils.rm_rf(@dump_path)
        else
          raise Errors::Database::PipelineError,
            "#{ @name } Failed to create compressed dump package:\n" +
            "'#{ outfile }'\n" +
            pipeline.error_messages
        end
      end
    end
  end
end
