package codes.chia7712.hellodb.admin;

import codes.chia7712.hellodb.Table;
import codes.chia7712.hellodb.data.Cell;
import codes.chia7712.hellodb.data.CellComparator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

class SimpleAdmin implements Admin {

  private final ConcurrentMap<String, Table> tables = new ConcurrentSkipListMap<>();

  SimpleAdmin(Properties prop) {
  }

  @Override
  public void createTable(String name) throws IOException {
    if (tables.containsKey(name)) {
      throw new IOException(name + " exists");
    }
    tables.computeIfAbsent(name, SimpleTable::new);
  }

  @Override
  public boolean tableExist(String name) throws IOException {
    return tables.containsKey(name);
  }

  @Override
  public void deleteTable(String name) throws IOException {
    if (tables.remove(name) == null) {
      throw new IOException(name + " not found");
    }
  }

  @Override
  public Table openTable(String name) throws IOException {
    Table t = tables.get(name);
    if (t == null) {
      throw new IOException(name + " not found");
    }
    return t;
  }

  @Override
  public List<String> listTables() throws IOException {
    return tables.keySet().stream().collect(Collectors.toList());
  }

  @Override
  public void close() throws IOException {
    //nothing
  }

  private static class SimpleTable implements Table {

    private static final CellComparator CELL_COMPARATOR = new CellComparator();
    private final ConcurrentNavigableMap<Cell, Cell> data = new ConcurrentSkipListMap<>(CELL_COMPARATOR);
    private final String name;

    SimpleTable(final String name) {
      this.name = name;
    }

    @Override
    public boolean insert(Cell cell) throws IOException {
      return data.put(cell, cell) != null;
    }

    @Override
    public void delete(byte[] row) throws IOException {
      Cell rowOnlyCell = Cell.createRowOnly(row);
      for (Map.Entry<Cell, Cell> entry : data.tailMap(rowOnlyCell).entrySet()) {
        if (CellComparator.compareRow(entry.getKey(), rowOnlyCell) != 0) {
          return;
        } else {
          data.remove(entry.getKey());
        }
      }
    }

    @Override
    public Iterator<Cell> get(byte[] row) throws IOException {
      Cell rowOnlyCell = Cell.createRowOnly(row);
      List<Cell> rval = new ArrayList<>();
      for (Map.Entry<Cell, Cell> entry : data.tailMap(rowOnlyCell).entrySet()) {
        if (CellComparator.compareRow(entry.getKey(), rowOnlyCell) != 0) {
          break;
        } else {
          rval.add(entry.getValue());
        }
      }
      return rval.iterator();
    }

    @Override
    public Optional<Cell> get(byte[] row, byte[] column) throws IOException {
      return Optional.ofNullable(data.get(Cell.createRowColumnOnly(row, column)));
    }

    @Override
    public boolean delete(byte[] row, byte[] column) throws IOException {
      return data.remove(Cell.createRowColumnOnly(row, column)) != null;
    }

    @Override
    public boolean insertIfAbsent(Cell cell) throws IOException {
      return data.putIfAbsent(cell, cell) == null;
    }

    @Override
    public void close() throws IOException {
      //nothing
    }

    @Override
    public String getName() {
      return name;
    }

  }

}
