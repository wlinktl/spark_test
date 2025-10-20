"# spark_test" 


File directTest = new File("/tmp/abc");
LOGGER.info("Direct test - exists: {}", directTest.exists());
LOGGER.info("Direct test - isDirectory: {}", directTest.isDirectory());
LOGGER.info("Direct test - canRead: {}", directTest.canRead());

--------------------------------------------------------------
File jarLocation = resource.getFile();

// Critical debugging
LOGGER.info("=== Debug Info ===");
LOGGER.info("Original libPath: {}", libPath);
LOGGER.info("Resource description: {}", resource.getDescription());
LOGGER.info("Resource URL: {}", resource.getURL());
LOGGER.info("jarLocation.getAbsolutePath(): {}", jarLocation.getAbsolutePath());
LOGGER.info("jarLocation.getCanonicalPath(): {}", jarLocation.getCanonicalPath());
LOGGER.info("jarLocation.exists(): {}", jarLocation.exists());
LOGGER.info("jarLocation.isFile(): {}", jarLocation.isFile());
LOGGER.info("jarLocation.isDirectory(): {}", jarLocation.isDirectory());
LOGGER.info("jarLocation.canRead(): {}", jarLocation.canRead());
LOGGER.info("==================");
