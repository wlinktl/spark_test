"# spark_test" 


<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-surefire-plugin</artifactId>
    <version>2.22.0</version>
    <configuration>
        <forkCount>4</forkCount>
        <reuseForks>false</reuseForks>
        <argLine>
            -Djavax.jdo.option.ConnectionURL=jdbc:h2:mem:testdb${surefire.forkNumber};MODE=MySQL;DB_CLOSE_DELAY=-1
            -Dhive.metastore.warehouse.dir=${project.build.directory}/spark-warehouse-${surefire.forkNumber}
        </argLine>
    </configuration>
</plugin>
