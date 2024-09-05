package org.welyss.binlog.parser;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.shyiko.mysql.binlog.BinaryLogFileReader;
import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;

public class BinlogExecutor {
	public static final Logger logger = LogManager.getLogger();

	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		if (args.length > 0) {
			Options options = new Options();
			Option databaseOpt = new Option("d", "database", true, "Database filter");
			options.addOption(databaseOpt);
			Option tableOpt = new Option("t", "table", true, "Table filter");
			options.addOption(tableOpt);
			Option startBinlogPositionOpt = new Option("j", "start-position", true,
					"Start reading the binlog at position N");
			options.addOption(startBinlogPositionOpt);
			Option stopBinlogPositionOpt = new Option("k", "stop-position", true,
					"Stop reading the binlog at position N");
			options.addOption(stopBinlogPositionOpt);
			Option whereOpt = new Option("w", "where", true, "Condition in where for filtering data");
			options.addOption(whereOpt);
			Option showPosOpt = new Option("s", "show-position", false, "Show position");
			options.addOption(showPosOpt);
			CommandLineParser parser = new DefaultParser();
			HelpFormatter formatter = new HelpFormatter();
			CommandLine cmd;
			try {
				cmd = parser.parse(options, args);
				String databaseFilter = cmd.getOptionValue(databaseOpt);
				String tableFilter = cmd.getOptionValue(tableOpt);
				String whereFilter = cmd.getOptionValue(whereOpt);
				long startPos = -1;
				try {
					startPos = Long.parseLong(cmd.getOptionValue(startBinlogPositionOpt));
				} catch (Exception e) {}
				long stopPos = Long.MAX_VALUE;
				try {
					stopPos = Long.parseLong(cmd.getOptionValue(stopBinlogPositionOpt));
				} catch (Exception e) {}
				boolean showPosition = cmd.hasOption(showPosOpt);
				String binlogFile = args[args.length - 1];
				if (binlogFile != null && !binlogFile.startsWith("-")) {
					EventDeserializer eventDeserializer = new EventDeserializer();
//					eventDeserializer.setCompatibilityMode(EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG,
//							EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY);
					BinaryLogFileReader reader;
					try {
						reader = new BinaryLogFileReader(new File(binlogFile), eventDeserializer);
						try {
							Map<Long, TableMapEventData> tableMapEventByTableId;
							Field field;
							try {
								field = eventDeserializer.getClass().getDeclaredField("tableMapEventByTableId");
								field.setAccessible(true);
								tableMapEventByTableId = (Map<Long, TableMapEventData>) field.get(eventDeserializer);
							} catch (NoSuchFieldException | SecurityException | IllegalArgumentException
									| IllegalAccessException e) {
								System.out.println("Table map event by table id faild.");
								return;
							}
							for (Event event; (event = reader.readEvent()) != null;) {
								EventHeaderV4 eh = (EventHeaderV4) event.getHeader();
								if (eh.getPosition() > stopPos) {
									break;
								}
								if (startPos <= eh.getPosition() && eh.getPosition() <= stopPos) {
									EventType type = eh.getEventType();
									if (EventType.isRowMutation(type)) {
										long tableId;
										// INSERT
										if (EventType.isWrite(type)) {
											WriteRowsEventData writeEventData = event.getData();
											tableId = writeEventData.getTableId();
											// UPDATE
										} else if (EventType.isUpdate(type)) {
											UpdateRowsEventData updateEventData = event.getData();
											tableId = updateEventData.getTableId();
											// DELETE
										} else if (EventType.isDelete(type)) {
											DeleteRowsEventData deleteEventData = event.getData();
											tableId = deleteEventData.getTableId();
										} else {
											tableId = -1;
											System.out.println("Unknown event type.");
										}
										TableMapEventData tmed = tableMapEventByTableId.get(tableId);
										String database = tmed.getDatabase();
										String table = tmed.getTable();
										if (databaseFilter != null && databaseFilter.trim().length() > 0
												&& !databaseFilter.equals(database)) {
											continue;
										} else if (tableFilter != null && tableFilter.trim().length() > 0
												&& !tableFilter.equals(table)) {
											continue;
										}
										StringBuilder sqlBuff = new StringBuilder();
										// where filter ready
										int whereColIndex = -1;
										String value = null;
										if (whereFilter != null && whereFilter.trim().length() > 0) {
											String[] condition = whereFilter.split("=");
											String column = condition[0];
											if (column != null && column.startsWith("@")) {
												try {
													whereColIndex = Integer.parseInt(column.substring(1));
												} catch (NumberFormatException nfe) {
												}
											}
											value = condition[1];
										}
										if (EventType.isUpdate(type)) {
											UpdateRowsEventData updateEventData = event.getData();
											// update
											List<Entry<Serializable[], Serializable[]>> rows = updateEventData.getRows();
											if (rows.size() > 0) {
												
												for (int ri = 0; ri < rows.size(); ri++) {
													Entry<Serializable[], Serializable[]> pair = rows.get(ri);
													boolean skip = false;
													StringBuilder sb = new StringBuilder();
													StringBuilder whereBuff = new StringBuilder();
													sb.append("UPDATE `").append(database).append("`.`").append(table).append("` SET ");
													Serializable[] before = pair.getKey();
													Serializable[] after = pair.getValue();
													for (int i = 0; i < before.length; i++) {
														Object beforeVal = convertVal(before[i]);
														Object afterVal = convertVal(after[i]);
														if (i + 1 == whereColIndex) {
															if (beforeVal != null && !beforeVal.toString().equals(value)) {
																skip = true;
																break;
															}
														}
														sb.append("@").append(i + 1).append("=").append(formatVal(afterVal));
														whereBuff.append("@").append(i + 1).append("=").append(formatVal(beforeVal));
														if (i < before.length - 1) {
															sb.append(",");
															whereBuff.append(" AND ");
														}
													}
													sb.append(" WHERE ").append(whereBuff);
													if (!skip) {
														sqlBuff.append(sb);
														if (ri < rows.size() - 1) {
															sqlBuff.append("\n");
														}
													}
												}
											}
										} else {
											List<Serializable[]> rows;
											if (EventType.isWrite(type)) {
												WriteRowsEventData writeEventData = event.getData();
												// insert
												rows = writeEventData.getRows();
												for (int ri = 0; ri < rows.size(); ri++) {
													Serializable[] row = rows.get(ri);
													boolean skip = false;
													StringBuilder sb = new StringBuilder();
													sb.append("INSERT INTO `").append(database).append("`.`").append(table).append("` VALUES(");
													for (int i = 0; i < row.length; i++) {
														Object item = convertVal(row[i]);
														if (i + 1 == whereColIndex) {
															if (item != null && !item.toString().equals(value)) {
																skip = true;
																break;
															}
														}
														sb.append(formatVal(item));
														if (i < row.length - 1) {
															sb.append(",");
														}
													}
													sb.append(")");
													if (!skip) {
														sqlBuff.append(sb);
														if (ri < rows.size() - 1) {
															sqlBuff.append("\n");
														}
													}
												}
											} else if (EventType.isDelete(type)) {
												DeleteRowsEventData deleteEventData = event.getData();
												// delete
												rows = deleteEventData.getRows();
												for (int ri = 0; ri < rows.size(); ri++) {
													Serializable[] row = rows.get(ri);
													boolean skip = false;
													StringBuilder sb = new StringBuilder();
													sb.append("DELETE FROM `").append(database).append("`.`").append(table).append("` WHERE ");
													for (int i = 0; i < row.length; i++) {
														Object item = convertVal(row[i]);
														if (i + 1 == whereColIndex) {
															if (item != null && !item.toString().equals(value)) {
																skip = true;
																break;
															}
														}
														sb.append("@").append(i + 1).append("=").append(formatVal(item));
														if (i < row.length - 1) {
															sb.append(" AND");
														}
													}
													if (!skip) {
														sqlBuff.append(sb);
														if (ri < rows.size() - 1) {
															sqlBuff.append("\n");
														}
													}
												}
											}
										}
										// output
										String output = sqlBuff.toString().trim();
										if (output.length()>0) {
											if (showPosition) {
												sqlBuff.insert(0, "/* POSITION:" + eh.getPosition() + " */ ");
											}
											System.out.println(sqlBuff);
										}
									}
								}
							}
						} finally {
							reader.close();
						}
					} catch (IOException e) {
//				logger.error("{}", e.getMessage());
						System.out.println(e.getMessage());
					}
				} else {
					System.out.println("binlog file is required.");
				}
			} catch (ParseException e) {
				System.out.println("invalid param, reason: " + e.getMessage());
				formatter.printHelp("param-name", options);
			}
		} else {
			System.out.println("param binlog file is required");
		}
	}

	private static Object convertVal(Object val) {
		Object result = null;
		if (val != null) {
			if (val instanceof byte[]) {
				result = new String((byte[])val);
			} else {
				result = val;
			}
		}
		return result;
	}

	private static String formatVal(Object val) {
		String result;
		if (val == null) {
			result = null;
		} else {
			if (val instanceof String || val instanceof Date) {
				result = "'" + val + "'";
			} else {
				result = val.toString();
			}
		}
		return result;
	}
}
