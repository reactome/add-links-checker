package org.reactome;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.gk.model.GKInstance;
import org.gk.model.ReactomeJavaConstants;
import org.gk.persistence.MySQLAdaptor;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Joel Weiser (joel.weiser@oicr.on.ca)
 *         Created 10/24/2022
 */
public class AddLinksChecker {
    @Parameter(names={"--newDatabaseName", "-n"}, required=true,
        description="More recent database to check reference databases and referrer counts")
    private String newDatabaseName;
    @Parameter(names={"--oldDatabaseName", "-o"}, required=true,
        description="Older database to check reference databases and referrer counts")
    private String oldDatabaseName;

    public static void main(String[] args) throws Exception {
        AddLinksChecker addLinksChecker = new AddLinksChecker();
        JCommander.newBuilder()
            .addObject(addLinksChecker)
            .build()
            .parse(args);

        ReferenceDatabaseComparisonResults referenceDatabaseComparisonResults =
            addLinksChecker.compareReferenceDatabases();

        System.out.println(referenceDatabaseComparisonResults.getReport());
    }

    public ReferenceDatabaseComparisonResults compareReferenceDatabases() throws Exception {
        Map<String, GKInstance> currentNameToReferenceDatabase =
            getReferenceDatabaseNameToInstance(getCurrentDBA(newDatabaseName));
        Map<String, GKInstance> previousNameToReferenceDatabase =
            getReferenceDatabaseNameToInstance(getPreviousDBA(oldDatabaseName));

        ReferenceDatabaseComparisonResults referenceDatabaseComparisonResults =
            new ReferenceDatabaseComparisonResults();

        for (String previousReferenceDatabaseName : getSortedNames(previousNameToReferenceDatabase.keySet())) {
            GKInstance currentReferenceDatabase = currentNameToReferenceDatabase.get(previousReferenceDatabaseName);
            GKInstance previousReferenceDatabase = previousNameToReferenceDatabase.get(previousReferenceDatabaseName);

            if (currentReferenceDatabase == null) {
                referenceDatabaseComparisonResults.addMissingReferenceDatabase(previousReferenceDatabaseName);
            } else if (referrerCount(previousReferenceDatabase) > referrerCount(currentReferenceDatabase)) {
                referenceDatabaseComparisonResults.addReducedCountReferenceDatabase(
                    currentReferenceDatabase, previousReferenceDatabase);
            } else {
                referenceDatabaseComparisonResults.addProperCountReferenceDatabase(
                    currentReferenceDatabase,previousReferenceDatabase);
            }
        }
        return referenceDatabaseComparisonResults;
    }

    private static Map<String, GKInstance> getReferenceDatabaseNameToInstance(MySQLAdaptor dba) throws Exception {
        return getReferenceDatabases(dba).stream().collect(Collectors.toMap(
            AddLinksChecker::getLongestName,
            referenceDatabase -> referenceDatabase
        ));
    }

    private static List<String> getSortedNames(Collection<String> unsortedNames) {
        List<String> namesList = new ArrayList<>(unsortedNames);
        namesList.sort(Comparator.comparing(String::toLowerCase));
        return namesList;
    }

    private static int referrerCount(GKInstance referenceDatabase) throws Exception {
        return referenceDatabase.getReferers(ReactomeJavaConstants.referenceDatabase).size();
    }

    private static String getNameWithDbId(GKInstance instance) {
        return instance.getDisplayName().equals(getLongestName(instance)) ?
            instance.getExtendedDisplayName() :
            String.format("[%s:%d] %s",
                instance.getSchemClass().getName(),
                instance.getDBID(),
                getLongestName(instance)
            );
    }

    @SuppressWarnings("unchecked")
    private static String getLongestName(GKInstance instance) {
        try {
            return ((List<String>) instance.getAttributeValuesList(ReactomeJavaConstants.name))
                .stream()
                .reduce("",(name1, name2) -> (name1.length() > name2.length()) ? name1 : name2);
        } catch (Exception e) {
            throw new RuntimeException("Unable to get names for instance " + instance);
        }
    }

    @SuppressWarnings("unchecked")
    private static List<GKInstance> getReferenceDatabases(MySQLAdaptor dba) throws Exception {
        return new ArrayList<GKInstance>(
            dba.fetchInstancesByClass(ReactomeJavaConstants.ReferenceDatabase)
        );
    }

    private static MySQLAdaptor getCurrentDBA(String dbName) throws SQLException, IOException {
        return getDBA("current", dbName);
    }

    private static MySQLAdaptor getPreviousDBA(String dbName) throws SQLException, IOException {
        return getDBA("previous", dbName);
    }

    private static MySQLAdaptor getDBA(String configPrefix, String defaultDbName) throws SQLException, IOException {
        Properties configProperties = getConfigProperties();

        String host = configProperties.getProperty(configPrefix + "DbHost", "localhost");
        String dbName = configProperties.getProperty(configPrefix + "DbName", defaultDbName);

        String user = takeFirstNotNull(
            configProperties.getProperty(configPrefix + "DbUser"),
            configProperties.getProperty("dbUser"),
            "root"
        );
        String password = takeFirstNotNull(
            configProperties.getProperty(configPrefix + "DbPass"),
            configProperties.getProperty("dbPwd"),
            "root"
        );

        return new MySQLAdaptor(host, dbName, user, password);
    }

    private static Properties getConfigProperties() throws IOException {
        Properties configProperties = new Properties();
        configProperties.load(AddLinksChecker.class.getClassLoader().getResourceAsStream("config.properties"));
        return configProperties;
    }

    private static String takeFirstNotNull(String ...values) {
        return Arrays.stream(values).filter(Objects::nonNull).findFirst().orElse(null);
    }

    public class ReferenceDatabaseComparisonResults {
        private List<String> missingReferenceDatabaseMessages;
        private List<String> reducedCountReferenceDatabaseMessages;
        private List<String> properCountReferenceDatabaseMessages;

        public ReferenceDatabaseComparisonResults() {
            this.missingReferenceDatabaseMessages = new ArrayList<>();
            this.reducedCountReferenceDatabaseMessages = new ArrayList<>();
            this.properCountReferenceDatabaseMessages = new ArrayList<>();
        }

        void addMissingReferenceDatabase(String missingReferenceDatabase) {
            this.missingReferenceDatabaseMessages.add(
                "ERROR: " + missingReferenceDatabase + " is missing in the current database"
            );
        }

        void addReducedCountReferenceDatabase(
            GKInstance currentReferenceDatabase, GKInstance previousReferenceDatabase) throws Exception {

            this.reducedCountReferenceDatabaseMessages.add(
                String.format(
                    "WARN: %s has a lower referrer count than previously: %s (current) vs %s (previous)",
                    getNameWithDbId(currentReferenceDatabase),
                    referrerCount(currentReferenceDatabase),
                    referrerCount(previousReferenceDatabase)
                )
            );
        }

        void addProperCountReferenceDatabase(
            GKInstance currentReferenceDatabase, GKInstance previousReferenceDatabase) throws Exception {
            this.properCountReferenceDatabaseMessages.add(
                String.format(
                    "%s - Current: %s; Previous %s",
                    getNameWithDbId(currentReferenceDatabase),
                    referrerCount(currentReferenceDatabase),
                    referrerCount(previousReferenceDatabase)
                )
            );
        }

        int referrerCount(GKInstance referenceDatabase) throws Exception {
            return referenceDatabase.getReferers(ReactomeJavaConstants.referenceDatabase).size();
        }

        String getReport() {
            StringBuilder reportBuilder = new StringBuilder();

            reportBuilder.append(addReportSection(
                "Missing Reference Databases:", this.missingReferenceDatabaseMessages));
            reportBuilder.append(addReportSection(
                "Reduced Count Reference Databases:", this.reducedCountReferenceDatabaseMessages));
            reportBuilder.append(addReportSection(
                "Proper Count Reference Databases:", this.properCountReferenceDatabaseMessages));

            return reportBuilder.toString();
        }

        String addReportSection(String sectionHeader, List<String> messages) {
            StringBuilder reportSectionBuilder = new StringBuilder();
            reportSectionBuilder.append(sectionHeader);
            reportSectionBuilder.append(System.lineSeparator());
            reportSectionBuilder.append(System.lineSeparator());
            for (String message : messages) {
                reportSectionBuilder.append(message);
                reportSectionBuilder.append(System.lineSeparator());
            }
            reportSectionBuilder.append(System.lineSeparator());
            return reportSectionBuilder.toString();
        }
    }
}