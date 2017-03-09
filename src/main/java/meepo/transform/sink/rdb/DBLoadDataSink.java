package meepo.transform.sink.rdb;

/**
 * Created by peiliping on 17-3-9.
 */
public class DBLoadDataSink {

//    protected boolean sendData(final List<Object[]> datas) {
//        StringBuilder builder = new StringBuilder();
//        for (Object[] o : datas) {
//            for (int i = 0; i < o.length; i++) {
//                builder.append(o[i]);
//                builder.append(i == o.length - 1 ? "\n" : "\t");
//            }
//        }
//        byte[] bytes = builder.toString().getBytes();
//        InputStream is = new ByteArrayInputStream(bytes);
//        return BasicDao.excuteLoadData(config.getTargetDataSource(), SQL, is);
//    }
//
//    @Override protected String buildSQL() {
//        return "LOAD DATA LOCAL INFILE 'sql.csv' INTO TABLE " + config.getTargetTableName() + " FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' ";
//    }

}
