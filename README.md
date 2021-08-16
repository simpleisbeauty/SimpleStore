SimpleStore is open source simple store similar to AWS S3, which has store input/output stream can be used for any application to switch from local IO to directly cloud IO.


store-io: Store IO interface with Cassandra implementation with following features
          1. File is stored in compressed chunks distributed in cloud with concurrent read/write for fast speed; also random read text file. Implemented
             StoreInput/OutputStream which can be easily and widely integrated to any Java application.
          2. File is always stored as unique version - multiple concurrent creation of same file name will result in multiple same file versions, so no
             need to lock for confliction in create/copy/move operation, simple and fast. Get file default is the latest version.
          3. Even file operation doesn't need lock, store still provide lock/trylock/unlock to synchronize any store wide operation.
          4. File deletion is soft - only set inactive flag, and TTL(time to live) of each version, so it's easy to roll back(restore). After deletion TTL
             expires, system would automatically set the version as purgeable(can't view/restore any more). Also, deletion TTL and max versions are all
             configurable, extra version would be automatically soft deleted.
          5. Provide filter to show deleted files; interface to delete file specific version, entire file, or recursively dir tree; and rollback all
             delete operations.
          6. File copy is hard (no soft) link - only create new destination file metadata referencing existing data chunks, which only delete after no
             metadata reference it. So, file copy is saving storage, and very fast.
          7. Both file copy and move leave out inactive version, because it's virtual not seen, but it can still can be done by version copy/move until
             it's purged.
          8. As most object stores, there is no actual dir create/delete, because it just simulates file system using key in path format for better view/
             manage files. To search/list file fast in large volume store, Simple dir tree is maintained in parent_dir:subdir map using store lock,
             without any consideration of files, because parent_dir is part of store primary key, which can easy and fast list files in it.
          9. Use store database as file bucket with its own configuration, similar to AWS s3.
         10. Have store housekeeping tasks run in store wide singleton with changeable time interval backed by store lock and runtime refreshable latest
             configuration (util/LatestConfig).
         11. Auto increasing/decreasing Connection pool size, and runtime changeable pool configuration to dynamically tune store performance.


store-service: Store Restful Service wrapping store-io for any UI or application integration
               mvn package, copy store.war to any Java Servlet Container webapps folder, e.g. http://localhost:8080/store/api/ to manage any store e.g. maven2
               create bucket: curl -X POST -H "act:create_store" http://localhost:8080/store/api/maven2/
               delete bucket: curl -X DELETE -H "act:delete_store" http://localhost:8080/store/api/maven2/
               housekeeping: curl -X DELETE -H "act:housekeeping" http://localhost:8080/store/api/maven2/

               put file: curl -X PUT http://localhost:8080/store/api/maven2/home/johnny/gig.zip --data-binary '@/home/johnny/gig-size.zip'
                 {"store":"maven2","parent":"/home/johnny/", "name":"gig.zip", "id":"0b44ae10-edc6-11eb-9eae-b996243239b0", "size":1509949440,
                  "chunks":144, "created":1628279009053411, "active":true}
               copy version: curl -X POST -H "act:copy_ver" -H "id:0b44ae10-edc6-11eb-9eae-b996243239b0" -H "dest:/home/dev/gig.zip" http://localhost:8080/store/api/maven2/home/johnny/gig.zip
               copy file(all version): curl -X POST -H "act:copy" -H "dest:/home/dev/gig.zip" http://localhost:8080/store/api/maven2/home/johnny/gig.zip
               copy dir recursively: curl -X POST -H "act:copy_recursive" -H "dest:/home/apps/" http://localhost:8080/store/api/maven2/home/johnny/

               move version: curl -X POST -H "act:move_ver" -H "id:0b44ae10-edc6-11eb-9eae-b996243239b0" -H "dest:/home/dev2/gig.zip" http://localhost:8080/store/api/maven2/home/johnny/gig.zip
               move file(all version): curl -X POST -H "act:move" -H "dest:/home/dev/gig.zip" http://localhost:8080/store/api/maven2/home/dev2/gig.zip
               move dir recursively: curl -X POST -H "act:move_recursive" -H "dest:/home/dev4/" http://localhost:8080/store/api/maven2/home/apps/

               delete version: curl -X DELETE -H "act:delete_ver" -H "id:bf04cdd0-edc7-11eb-8f40-49e3ff91c841" http://localhost:8080/store/api/maven2/home/apps/gig.zip
               restore version: curl -X DELETE -H "act:restore_ver" -H "id:bf04cdd0-edc7-11eb-8f40-49e3ff91c841" http://localhost:8080/store/api/maven2/home/apps/gig.zip
               delete file: curl -X DELETE http://localhost:8080/store/api/maven2/home/apps/gig.zip
               restore file: curl -X DELETE -H "act:restore" http://localhost:8080/store/api/maven2/home/apps/gig.zip
               delete dir recursively: curl -X DELETE -H "act:delete_recursive" http://localhost:8080/store/api/maven2/home/
               restore dir recursively: curl -X DELETE -H "act:restore_recursive" http://localhost:8080/store/api/maven2/home/

               get file (latest version): curl -X GET http://localhost:8080/store/api/maven2/home/johnny/gig.zip
               get file version: curl -X GET -H "id:bf04cdd0-edc7-11eb-8f40-49e3ff91c841" http://localhost:8080/store/api/maven2/home/johnny/gig.zip
               list dir: curl -X GET http://localhost:8080/store/api/maven2/home/
               list dir (include inactive): curl -X GET -H "show_deleted:true" http://localhost:8080/store/api/maven2/home/


maven-repo: Maven repo based on store-io, also has a simple local file system repo
            1. mvn package, copy maven2.war to any Java Servlet Container webapps folder, e.g. http://localhost:8080/maven2/repo/ to list all packages
            2. to publish package, like store-io, run mvn deploy:deploy-file -DpomFile=./pom.xml -Dfile=target/store-io-1.0.jar -DrepositoryId=simple-repo -Durl=http://localhost:8080/maven2/repo
            3. In store-io's dependent pom.xml, start to use the maven repo:
                   <repository>
                       <id>store-repo</id>
                       <name>shared releases</name>
                       <url>http://localhost:8080/maven2/repo</url>
                   </repository>
            4. Use store-service (store.war) to manage the maven2 repo.
