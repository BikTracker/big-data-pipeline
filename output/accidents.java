// ORM class for table 'accidents'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Sat May 11 15:01:17 MSK 2024
// For connector: org.apache.sqoop.manager.PostgresqlManager
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;
import org.apache.sqoop.lib.JdbcWritableBridge;
import org.apache.sqoop.lib.DelimiterSet;
import org.apache.sqoop.lib.FieldFormatter;
import org.apache.sqoop.lib.RecordParser;
import org.apache.sqoop.lib.BooleanParser;
import org.apache.sqoop.lib.BlobRef;
import org.apache.sqoop.lib.ClobRef;
import org.apache.sqoop.lib.LargeObjectLoader;
import org.apache.sqoop.lib.SqoopRecord;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

public class accidents extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  public static interface FieldSetterCommand {    void setField(Object value);  }  protected ResultSet __cur_result_set;
  private Map<String, FieldSetterCommand> setters = new HashMap<String, FieldSetterCommand>();
  private void init0() {
    setters.put("id", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        accidents.this.id = (String)value;
      }
    });
    setters.put("source", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        accidents.this.source = (String)value;
      }
    });
    setters.put("severity", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        accidents.this.severity = (Integer)value;
      }
    });
    setters.put("start_time", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        accidents.this.start_time = (String)value;
      }
    });
    setters.put("end_time", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        accidents.this.end_time = (String)value;
      }
    });
    setters.put("start_lat", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        accidents.this.start_lat = (Double)value;
      }
    });
    setters.put("start_lng", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        accidents.this.start_lng = (Double)value;
      }
    });
    setters.put("end_lat", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        accidents.this.end_lat = (Double)value;
      }
    });
    setters.put("end_lng", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        accidents.this.end_lng = (Double)value;
      }
    });
    setters.put("distance", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        accidents.this.distance = (Double)value;
      }
    });
    setters.put("description", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        accidents.this.description = (String)value;
      }
    });
    setters.put("street", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        accidents.this.street = (String)value;
      }
    });
    setters.put("city", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        accidents.this.city = (String)value;
      }
    });
    setters.put("county", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        accidents.this.county = (String)value;
      }
    });
    setters.put("state", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        accidents.this.state = (String)value;
      }
    });
    setters.put("zipcode", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        accidents.this.zipcode = (String)value;
      }
    });
    setters.put("country", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        accidents.this.country = (String)value;
      }
    });
    setters.put("timezone", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        accidents.this.timezone = (String)value;
      }
    });
    setters.put("airport_code", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        accidents.this.airport_code = (String)value;
      }
    });
    setters.put("weather_timestamp", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        accidents.this.weather_timestamp = (String)value;
      }
    });
    setters.put("temperature", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        accidents.this.temperature = (Double)value;
      }
    });
    setters.put("wind_chill", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        accidents.this.wind_chill = (Double)value;
      }
    });
    setters.put("humidity", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        accidents.this.humidity = (Double)value;
      }
    });
    setters.put("pressure", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        accidents.this.pressure = (Double)value;
      }
    });
    setters.put("visibility", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        accidents.this.visibility = (Double)value;
      }
    });
    setters.put("wind_direction", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        accidents.this.wind_direction = (String)value;
      }
    });
    setters.put("wind_speed", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        accidents.this.wind_speed = (Double)value;
      }
    });
    setters.put("precipitation", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        accidents.this.precipitation = (Double)value;
      }
    });
    setters.put("weather_condition", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        accidents.this.weather_condition = (String)value;
      }
    });
    setters.put("amenity", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        accidents.this.amenity = (Boolean)value;
      }
    });
    setters.put("bump", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        accidents.this.bump = (Boolean)value;
      }
    });
    setters.put("crossing", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        accidents.this.crossing = (Boolean)value;
      }
    });
    setters.put("give_way", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        accidents.this.give_way = (Boolean)value;
      }
    });
    setters.put("junction", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        accidents.this.junction = (Boolean)value;
      }
    });
    setters.put("no_exit", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        accidents.this.no_exit = (Boolean)value;
      }
    });
    setters.put("railway", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        accidents.this.railway = (Boolean)value;
      }
    });
    setters.put("roundabout", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        accidents.this.roundabout = (Boolean)value;
      }
    });
    setters.put("station", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        accidents.this.station = (Boolean)value;
      }
    });
    setters.put("stop", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        accidents.this.stop = (Boolean)value;
      }
    });
    setters.put("traffic_calming", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        accidents.this.traffic_calming = (Boolean)value;
      }
    });
    setters.put("traffic_signal", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        accidents.this.traffic_signal = (Boolean)value;
      }
    });
    setters.put("turning_loop", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        accidents.this.turning_loop = (Boolean)value;
      }
    });
    setters.put("sunrise_sunset", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        accidents.this.sunrise_sunset = (String)value;
      }
    });
    setters.put("civil_twilight", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        accidents.this.civil_twilight = (String)value;
      }
    });
    setters.put("nautical_twilight", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        accidents.this.nautical_twilight = (String)value;
      }
    });
    setters.put("astronomical_twilight", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        accidents.this.astronomical_twilight = (String)value;
      }
    });
  }
  public accidents() {
    init0();
  }
  private String id;
  public String get_id() {
    return id;
  }
  public void set_id(String id) {
    this.id = id;
  }
  public accidents with_id(String id) {
    this.id = id;
    return this;
  }
  private String source;
  public String get_source() {
    return source;
  }
  public void set_source(String source) {
    this.source = source;
  }
  public accidents with_source(String source) {
    this.source = source;
    return this;
  }
  private Integer severity;
  public Integer get_severity() {
    return severity;
  }
  public void set_severity(Integer severity) {
    this.severity = severity;
  }
  public accidents with_severity(Integer severity) {
    this.severity = severity;
    return this;
  }
  private String start_time;
  public String get_start_time() {
    return start_time;
  }
  public void set_start_time(String start_time) {
    this.start_time = start_time;
  }
  public accidents with_start_time(String start_time) {
    this.start_time = start_time;
    return this;
  }
  private String end_time;
  public String get_end_time() {
    return end_time;
  }
  public void set_end_time(String end_time) {
    this.end_time = end_time;
  }
  public accidents with_end_time(String end_time) {
    this.end_time = end_time;
    return this;
  }
  private Double start_lat;
  public Double get_start_lat() {
    return start_lat;
  }
  public void set_start_lat(Double start_lat) {
    this.start_lat = start_lat;
  }
  public accidents with_start_lat(Double start_lat) {
    this.start_lat = start_lat;
    return this;
  }
  private Double start_lng;
  public Double get_start_lng() {
    return start_lng;
  }
  public void set_start_lng(Double start_lng) {
    this.start_lng = start_lng;
  }
  public accidents with_start_lng(Double start_lng) {
    this.start_lng = start_lng;
    return this;
  }
  private Double end_lat;
  public Double get_end_lat() {
    return end_lat;
  }
  public void set_end_lat(Double end_lat) {
    this.end_lat = end_lat;
  }
  public accidents with_end_lat(Double end_lat) {
    this.end_lat = end_lat;
    return this;
  }
  private Double end_lng;
  public Double get_end_lng() {
    return end_lng;
  }
  public void set_end_lng(Double end_lng) {
    this.end_lng = end_lng;
  }
  public accidents with_end_lng(Double end_lng) {
    this.end_lng = end_lng;
    return this;
  }
  private Double distance;
  public Double get_distance() {
    return distance;
  }
  public void set_distance(Double distance) {
    this.distance = distance;
  }
  public accidents with_distance(Double distance) {
    this.distance = distance;
    return this;
  }
  private String description;
  public String get_description() {
    return description;
  }
  public void set_description(String description) {
    this.description = description;
  }
  public accidents with_description(String description) {
    this.description = description;
    return this;
  }
  private String street;
  public String get_street() {
    return street;
  }
  public void set_street(String street) {
    this.street = street;
  }
  public accidents with_street(String street) {
    this.street = street;
    return this;
  }
  private String city;
  public String get_city() {
    return city;
  }
  public void set_city(String city) {
    this.city = city;
  }
  public accidents with_city(String city) {
    this.city = city;
    return this;
  }
  private String county;
  public String get_county() {
    return county;
  }
  public void set_county(String county) {
    this.county = county;
  }
  public accidents with_county(String county) {
    this.county = county;
    return this;
  }
  private String state;
  public String get_state() {
    return state;
  }
  public void set_state(String state) {
    this.state = state;
  }
  public accidents with_state(String state) {
    this.state = state;
    return this;
  }
  private String zipcode;
  public String get_zipcode() {
    return zipcode;
  }
  public void set_zipcode(String zipcode) {
    this.zipcode = zipcode;
  }
  public accidents with_zipcode(String zipcode) {
    this.zipcode = zipcode;
    return this;
  }
  private String country;
  public String get_country() {
    return country;
  }
  public void set_country(String country) {
    this.country = country;
  }
  public accidents with_country(String country) {
    this.country = country;
    return this;
  }
  private String timezone;
  public String get_timezone() {
    return timezone;
  }
  public void set_timezone(String timezone) {
    this.timezone = timezone;
  }
  public accidents with_timezone(String timezone) {
    this.timezone = timezone;
    return this;
  }
  private String airport_code;
  public String get_airport_code() {
    return airport_code;
  }
  public void set_airport_code(String airport_code) {
    this.airport_code = airport_code;
  }
  public accidents with_airport_code(String airport_code) {
    this.airport_code = airport_code;
    return this;
  }
  private String weather_timestamp;
  public String get_weather_timestamp() {
    return weather_timestamp;
  }
  public void set_weather_timestamp(String weather_timestamp) {
    this.weather_timestamp = weather_timestamp;
  }
  public accidents with_weather_timestamp(String weather_timestamp) {
    this.weather_timestamp = weather_timestamp;
    return this;
  }
  private Double temperature;
  public Double get_temperature() {
    return temperature;
  }
  public void set_temperature(Double temperature) {
    this.temperature = temperature;
  }
  public accidents with_temperature(Double temperature) {
    this.temperature = temperature;
    return this;
  }
  private Double wind_chill;
  public Double get_wind_chill() {
    return wind_chill;
  }
  public void set_wind_chill(Double wind_chill) {
    this.wind_chill = wind_chill;
  }
  public accidents with_wind_chill(Double wind_chill) {
    this.wind_chill = wind_chill;
    return this;
  }
  private Double humidity;
  public Double get_humidity() {
    return humidity;
  }
  public void set_humidity(Double humidity) {
    this.humidity = humidity;
  }
  public accidents with_humidity(Double humidity) {
    this.humidity = humidity;
    return this;
  }
  private Double pressure;
  public Double get_pressure() {
    return pressure;
  }
  public void set_pressure(Double pressure) {
    this.pressure = pressure;
  }
  public accidents with_pressure(Double pressure) {
    this.pressure = pressure;
    return this;
  }
  private Double visibility;
  public Double get_visibility() {
    return visibility;
  }
  public void set_visibility(Double visibility) {
    this.visibility = visibility;
  }
  public accidents with_visibility(Double visibility) {
    this.visibility = visibility;
    return this;
  }
  private String wind_direction;
  public String get_wind_direction() {
    return wind_direction;
  }
  public void set_wind_direction(String wind_direction) {
    this.wind_direction = wind_direction;
  }
  public accidents with_wind_direction(String wind_direction) {
    this.wind_direction = wind_direction;
    return this;
  }
  private Double wind_speed;
  public Double get_wind_speed() {
    return wind_speed;
  }
  public void set_wind_speed(Double wind_speed) {
    this.wind_speed = wind_speed;
  }
  public accidents with_wind_speed(Double wind_speed) {
    this.wind_speed = wind_speed;
    return this;
  }
  private Double precipitation;
  public Double get_precipitation() {
    return precipitation;
  }
  public void set_precipitation(Double precipitation) {
    this.precipitation = precipitation;
  }
  public accidents with_precipitation(Double precipitation) {
    this.precipitation = precipitation;
    return this;
  }
  private String weather_condition;
  public String get_weather_condition() {
    return weather_condition;
  }
  public void set_weather_condition(String weather_condition) {
    this.weather_condition = weather_condition;
  }
  public accidents with_weather_condition(String weather_condition) {
    this.weather_condition = weather_condition;
    return this;
  }
  private Boolean amenity;
  public Boolean get_amenity() {
    return amenity;
  }
  public void set_amenity(Boolean amenity) {
    this.amenity = amenity;
  }
  public accidents with_amenity(Boolean amenity) {
    this.amenity = amenity;
    return this;
  }
  private Boolean bump;
  public Boolean get_bump() {
    return bump;
  }
  public void set_bump(Boolean bump) {
    this.bump = bump;
  }
  public accidents with_bump(Boolean bump) {
    this.bump = bump;
    return this;
  }
  private Boolean crossing;
  public Boolean get_crossing() {
    return crossing;
  }
  public void set_crossing(Boolean crossing) {
    this.crossing = crossing;
  }
  public accidents with_crossing(Boolean crossing) {
    this.crossing = crossing;
    return this;
  }
  private Boolean give_way;
  public Boolean get_give_way() {
    return give_way;
  }
  public void set_give_way(Boolean give_way) {
    this.give_way = give_way;
  }
  public accidents with_give_way(Boolean give_way) {
    this.give_way = give_way;
    return this;
  }
  private Boolean junction;
  public Boolean get_junction() {
    return junction;
  }
  public void set_junction(Boolean junction) {
    this.junction = junction;
  }
  public accidents with_junction(Boolean junction) {
    this.junction = junction;
    return this;
  }
  private Boolean no_exit;
  public Boolean get_no_exit() {
    return no_exit;
  }
  public void set_no_exit(Boolean no_exit) {
    this.no_exit = no_exit;
  }
  public accidents with_no_exit(Boolean no_exit) {
    this.no_exit = no_exit;
    return this;
  }
  private Boolean railway;
  public Boolean get_railway() {
    return railway;
  }
  public void set_railway(Boolean railway) {
    this.railway = railway;
  }
  public accidents with_railway(Boolean railway) {
    this.railway = railway;
    return this;
  }
  private Boolean roundabout;
  public Boolean get_roundabout() {
    return roundabout;
  }
  public void set_roundabout(Boolean roundabout) {
    this.roundabout = roundabout;
  }
  public accidents with_roundabout(Boolean roundabout) {
    this.roundabout = roundabout;
    return this;
  }
  private Boolean station;
  public Boolean get_station() {
    return station;
  }
  public void set_station(Boolean station) {
    this.station = station;
  }
  public accidents with_station(Boolean station) {
    this.station = station;
    return this;
  }
  private Boolean stop;
  public Boolean get_stop() {
    return stop;
  }
  public void set_stop(Boolean stop) {
    this.stop = stop;
  }
  public accidents with_stop(Boolean stop) {
    this.stop = stop;
    return this;
  }
  private Boolean traffic_calming;
  public Boolean get_traffic_calming() {
    return traffic_calming;
  }
  public void set_traffic_calming(Boolean traffic_calming) {
    this.traffic_calming = traffic_calming;
  }
  public accidents with_traffic_calming(Boolean traffic_calming) {
    this.traffic_calming = traffic_calming;
    return this;
  }
  private Boolean traffic_signal;
  public Boolean get_traffic_signal() {
    return traffic_signal;
  }
  public void set_traffic_signal(Boolean traffic_signal) {
    this.traffic_signal = traffic_signal;
  }
  public accidents with_traffic_signal(Boolean traffic_signal) {
    this.traffic_signal = traffic_signal;
    return this;
  }
  private Boolean turning_loop;
  public Boolean get_turning_loop() {
    return turning_loop;
  }
  public void set_turning_loop(Boolean turning_loop) {
    this.turning_loop = turning_loop;
  }
  public accidents with_turning_loop(Boolean turning_loop) {
    this.turning_loop = turning_loop;
    return this;
  }
  private String sunrise_sunset;
  public String get_sunrise_sunset() {
    return sunrise_sunset;
  }
  public void set_sunrise_sunset(String sunrise_sunset) {
    this.sunrise_sunset = sunrise_sunset;
  }
  public accidents with_sunrise_sunset(String sunrise_sunset) {
    this.sunrise_sunset = sunrise_sunset;
    return this;
  }
  private String civil_twilight;
  public String get_civil_twilight() {
    return civil_twilight;
  }
  public void set_civil_twilight(String civil_twilight) {
    this.civil_twilight = civil_twilight;
  }
  public accidents with_civil_twilight(String civil_twilight) {
    this.civil_twilight = civil_twilight;
    return this;
  }
  private String nautical_twilight;
  public String get_nautical_twilight() {
    return nautical_twilight;
  }
  public void set_nautical_twilight(String nautical_twilight) {
    this.nautical_twilight = nautical_twilight;
  }
  public accidents with_nautical_twilight(String nautical_twilight) {
    this.nautical_twilight = nautical_twilight;
    return this;
  }
  private String astronomical_twilight;
  public String get_astronomical_twilight() {
    return astronomical_twilight;
  }
  public void set_astronomical_twilight(String astronomical_twilight) {
    this.astronomical_twilight = astronomical_twilight;
  }
  public accidents with_astronomical_twilight(String astronomical_twilight) {
    this.astronomical_twilight = astronomical_twilight;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof accidents)) {
      return false;
    }
    accidents that = (accidents) o;
    boolean equal = true;
    equal = equal && (this.id == null ? that.id == null : this.id.equals(that.id));
    equal = equal && (this.source == null ? that.source == null : this.source.equals(that.source));
    equal = equal && (this.severity == null ? that.severity == null : this.severity.equals(that.severity));
    equal = equal && (this.start_time == null ? that.start_time == null : this.start_time.equals(that.start_time));
    equal = equal && (this.end_time == null ? that.end_time == null : this.end_time.equals(that.end_time));
    equal = equal && (this.start_lat == null ? that.start_lat == null : this.start_lat.equals(that.start_lat));
    equal = equal && (this.start_lng == null ? that.start_lng == null : this.start_lng.equals(that.start_lng));
    equal = equal && (this.end_lat == null ? that.end_lat == null : this.end_lat.equals(that.end_lat));
    equal = equal && (this.end_lng == null ? that.end_lng == null : this.end_lng.equals(that.end_lng));
    equal = equal && (this.distance == null ? that.distance == null : this.distance.equals(that.distance));
    equal = equal && (this.description == null ? that.description == null : this.description.equals(that.description));
    equal = equal && (this.street == null ? that.street == null : this.street.equals(that.street));
    equal = equal && (this.city == null ? that.city == null : this.city.equals(that.city));
    equal = equal && (this.county == null ? that.county == null : this.county.equals(that.county));
    equal = equal && (this.state == null ? that.state == null : this.state.equals(that.state));
    equal = equal && (this.zipcode == null ? that.zipcode == null : this.zipcode.equals(that.zipcode));
    equal = equal && (this.country == null ? that.country == null : this.country.equals(that.country));
    equal = equal && (this.timezone == null ? that.timezone == null : this.timezone.equals(that.timezone));
    equal = equal && (this.airport_code == null ? that.airport_code == null : this.airport_code.equals(that.airport_code));
    equal = equal && (this.weather_timestamp == null ? that.weather_timestamp == null : this.weather_timestamp.equals(that.weather_timestamp));
    equal = equal && (this.temperature == null ? that.temperature == null : this.temperature.equals(that.temperature));
    equal = equal && (this.wind_chill == null ? that.wind_chill == null : this.wind_chill.equals(that.wind_chill));
    equal = equal && (this.humidity == null ? that.humidity == null : this.humidity.equals(that.humidity));
    equal = equal && (this.pressure == null ? that.pressure == null : this.pressure.equals(that.pressure));
    equal = equal && (this.visibility == null ? that.visibility == null : this.visibility.equals(that.visibility));
    equal = equal && (this.wind_direction == null ? that.wind_direction == null : this.wind_direction.equals(that.wind_direction));
    equal = equal && (this.wind_speed == null ? that.wind_speed == null : this.wind_speed.equals(that.wind_speed));
    equal = equal && (this.precipitation == null ? that.precipitation == null : this.precipitation.equals(that.precipitation));
    equal = equal && (this.weather_condition == null ? that.weather_condition == null : this.weather_condition.equals(that.weather_condition));
    equal = equal && (this.amenity == null ? that.amenity == null : this.amenity.equals(that.amenity));
    equal = equal && (this.bump == null ? that.bump == null : this.bump.equals(that.bump));
    equal = equal && (this.crossing == null ? that.crossing == null : this.crossing.equals(that.crossing));
    equal = equal && (this.give_way == null ? that.give_way == null : this.give_way.equals(that.give_way));
    equal = equal && (this.junction == null ? that.junction == null : this.junction.equals(that.junction));
    equal = equal && (this.no_exit == null ? that.no_exit == null : this.no_exit.equals(that.no_exit));
    equal = equal && (this.railway == null ? that.railway == null : this.railway.equals(that.railway));
    equal = equal && (this.roundabout == null ? that.roundabout == null : this.roundabout.equals(that.roundabout));
    equal = equal && (this.station == null ? that.station == null : this.station.equals(that.station));
    equal = equal && (this.stop == null ? that.stop == null : this.stop.equals(that.stop));
    equal = equal && (this.traffic_calming == null ? that.traffic_calming == null : this.traffic_calming.equals(that.traffic_calming));
    equal = equal && (this.traffic_signal == null ? that.traffic_signal == null : this.traffic_signal.equals(that.traffic_signal));
    equal = equal && (this.turning_loop == null ? that.turning_loop == null : this.turning_loop.equals(that.turning_loop));
    equal = equal && (this.sunrise_sunset == null ? that.sunrise_sunset == null : this.sunrise_sunset.equals(that.sunrise_sunset));
    equal = equal && (this.civil_twilight == null ? that.civil_twilight == null : this.civil_twilight.equals(that.civil_twilight));
    equal = equal && (this.nautical_twilight == null ? that.nautical_twilight == null : this.nautical_twilight.equals(that.nautical_twilight));
    equal = equal && (this.astronomical_twilight == null ? that.astronomical_twilight == null : this.astronomical_twilight.equals(that.astronomical_twilight));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof accidents)) {
      return false;
    }
    accidents that = (accidents) o;
    boolean equal = true;
    equal = equal && (this.id == null ? that.id == null : this.id.equals(that.id));
    equal = equal && (this.source == null ? that.source == null : this.source.equals(that.source));
    equal = equal && (this.severity == null ? that.severity == null : this.severity.equals(that.severity));
    equal = equal && (this.start_time == null ? that.start_time == null : this.start_time.equals(that.start_time));
    equal = equal && (this.end_time == null ? that.end_time == null : this.end_time.equals(that.end_time));
    equal = equal && (this.start_lat == null ? that.start_lat == null : this.start_lat.equals(that.start_lat));
    equal = equal && (this.start_lng == null ? that.start_lng == null : this.start_lng.equals(that.start_lng));
    equal = equal && (this.end_lat == null ? that.end_lat == null : this.end_lat.equals(that.end_lat));
    equal = equal && (this.end_lng == null ? that.end_lng == null : this.end_lng.equals(that.end_lng));
    equal = equal && (this.distance == null ? that.distance == null : this.distance.equals(that.distance));
    equal = equal && (this.description == null ? that.description == null : this.description.equals(that.description));
    equal = equal && (this.street == null ? that.street == null : this.street.equals(that.street));
    equal = equal && (this.city == null ? that.city == null : this.city.equals(that.city));
    equal = equal && (this.county == null ? that.county == null : this.county.equals(that.county));
    equal = equal && (this.state == null ? that.state == null : this.state.equals(that.state));
    equal = equal && (this.zipcode == null ? that.zipcode == null : this.zipcode.equals(that.zipcode));
    equal = equal && (this.country == null ? that.country == null : this.country.equals(that.country));
    equal = equal && (this.timezone == null ? that.timezone == null : this.timezone.equals(that.timezone));
    equal = equal && (this.airport_code == null ? that.airport_code == null : this.airport_code.equals(that.airport_code));
    equal = equal && (this.weather_timestamp == null ? that.weather_timestamp == null : this.weather_timestamp.equals(that.weather_timestamp));
    equal = equal && (this.temperature == null ? that.temperature == null : this.temperature.equals(that.temperature));
    equal = equal && (this.wind_chill == null ? that.wind_chill == null : this.wind_chill.equals(that.wind_chill));
    equal = equal && (this.humidity == null ? that.humidity == null : this.humidity.equals(that.humidity));
    equal = equal && (this.pressure == null ? that.pressure == null : this.pressure.equals(that.pressure));
    equal = equal && (this.visibility == null ? that.visibility == null : this.visibility.equals(that.visibility));
    equal = equal && (this.wind_direction == null ? that.wind_direction == null : this.wind_direction.equals(that.wind_direction));
    equal = equal && (this.wind_speed == null ? that.wind_speed == null : this.wind_speed.equals(that.wind_speed));
    equal = equal && (this.precipitation == null ? that.precipitation == null : this.precipitation.equals(that.precipitation));
    equal = equal && (this.weather_condition == null ? that.weather_condition == null : this.weather_condition.equals(that.weather_condition));
    equal = equal && (this.amenity == null ? that.amenity == null : this.amenity.equals(that.amenity));
    equal = equal && (this.bump == null ? that.bump == null : this.bump.equals(that.bump));
    equal = equal && (this.crossing == null ? that.crossing == null : this.crossing.equals(that.crossing));
    equal = equal && (this.give_way == null ? that.give_way == null : this.give_way.equals(that.give_way));
    equal = equal && (this.junction == null ? that.junction == null : this.junction.equals(that.junction));
    equal = equal && (this.no_exit == null ? that.no_exit == null : this.no_exit.equals(that.no_exit));
    equal = equal && (this.railway == null ? that.railway == null : this.railway.equals(that.railway));
    equal = equal && (this.roundabout == null ? that.roundabout == null : this.roundabout.equals(that.roundabout));
    equal = equal && (this.station == null ? that.station == null : this.station.equals(that.station));
    equal = equal && (this.stop == null ? that.stop == null : this.stop.equals(that.stop));
    equal = equal && (this.traffic_calming == null ? that.traffic_calming == null : this.traffic_calming.equals(that.traffic_calming));
    equal = equal && (this.traffic_signal == null ? that.traffic_signal == null : this.traffic_signal.equals(that.traffic_signal));
    equal = equal && (this.turning_loop == null ? that.turning_loop == null : this.turning_loop.equals(that.turning_loop));
    equal = equal && (this.sunrise_sunset == null ? that.sunrise_sunset == null : this.sunrise_sunset.equals(that.sunrise_sunset));
    equal = equal && (this.civil_twilight == null ? that.civil_twilight == null : this.civil_twilight.equals(that.civil_twilight));
    equal = equal && (this.nautical_twilight == null ? that.nautical_twilight == null : this.nautical_twilight.equals(that.nautical_twilight));
    equal = equal && (this.astronomical_twilight == null ? that.astronomical_twilight == null : this.astronomical_twilight.equals(that.astronomical_twilight));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.id = JdbcWritableBridge.readString(1, __dbResults);
    this.source = JdbcWritableBridge.readString(2, __dbResults);
    this.severity = JdbcWritableBridge.readInteger(3, __dbResults);
    this.start_time = JdbcWritableBridge.readString(4, __dbResults);
    this.end_time = JdbcWritableBridge.readString(5, __dbResults);
    this.start_lat = JdbcWritableBridge.readDouble(6, __dbResults);
    this.start_lng = JdbcWritableBridge.readDouble(7, __dbResults);
    this.end_lat = JdbcWritableBridge.readDouble(8, __dbResults);
    this.end_lng = JdbcWritableBridge.readDouble(9, __dbResults);
    this.distance = JdbcWritableBridge.readDouble(10, __dbResults);
    this.description = JdbcWritableBridge.readString(11, __dbResults);
    this.street = JdbcWritableBridge.readString(12, __dbResults);
    this.city = JdbcWritableBridge.readString(13, __dbResults);
    this.county = JdbcWritableBridge.readString(14, __dbResults);
    this.state = JdbcWritableBridge.readString(15, __dbResults);
    this.zipcode = JdbcWritableBridge.readString(16, __dbResults);
    this.country = JdbcWritableBridge.readString(17, __dbResults);
    this.timezone = JdbcWritableBridge.readString(18, __dbResults);
    this.airport_code = JdbcWritableBridge.readString(19, __dbResults);
    this.weather_timestamp = JdbcWritableBridge.readString(20, __dbResults);
    this.temperature = JdbcWritableBridge.readDouble(21, __dbResults);
    this.wind_chill = JdbcWritableBridge.readDouble(22, __dbResults);
    this.humidity = JdbcWritableBridge.readDouble(23, __dbResults);
    this.pressure = JdbcWritableBridge.readDouble(24, __dbResults);
    this.visibility = JdbcWritableBridge.readDouble(25, __dbResults);
    this.wind_direction = JdbcWritableBridge.readString(26, __dbResults);
    this.wind_speed = JdbcWritableBridge.readDouble(27, __dbResults);
    this.precipitation = JdbcWritableBridge.readDouble(28, __dbResults);
    this.weather_condition = JdbcWritableBridge.readString(29, __dbResults);
    this.amenity = JdbcWritableBridge.readBoolean(30, __dbResults);
    this.bump = JdbcWritableBridge.readBoolean(31, __dbResults);
    this.crossing = JdbcWritableBridge.readBoolean(32, __dbResults);
    this.give_way = JdbcWritableBridge.readBoolean(33, __dbResults);
    this.junction = JdbcWritableBridge.readBoolean(34, __dbResults);
    this.no_exit = JdbcWritableBridge.readBoolean(35, __dbResults);
    this.railway = JdbcWritableBridge.readBoolean(36, __dbResults);
    this.roundabout = JdbcWritableBridge.readBoolean(37, __dbResults);
    this.station = JdbcWritableBridge.readBoolean(38, __dbResults);
    this.stop = JdbcWritableBridge.readBoolean(39, __dbResults);
    this.traffic_calming = JdbcWritableBridge.readBoolean(40, __dbResults);
    this.traffic_signal = JdbcWritableBridge.readBoolean(41, __dbResults);
    this.turning_loop = JdbcWritableBridge.readBoolean(42, __dbResults);
    this.sunrise_sunset = JdbcWritableBridge.readString(43, __dbResults);
    this.civil_twilight = JdbcWritableBridge.readString(44, __dbResults);
    this.nautical_twilight = JdbcWritableBridge.readString(45, __dbResults);
    this.astronomical_twilight = JdbcWritableBridge.readString(46, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.id = JdbcWritableBridge.readString(1, __dbResults);
    this.source = JdbcWritableBridge.readString(2, __dbResults);
    this.severity = JdbcWritableBridge.readInteger(3, __dbResults);
    this.start_time = JdbcWritableBridge.readString(4, __dbResults);
    this.end_time = JdbcWritableBridge.readString(5, __dbResults);
    this.start_lat = JdbcWritableBridge.readDouble(6, __dbResults);
    this.start_lng = JdbcWritableBridge.readDouble(7, __dbResults);
    this.end_lat = JdbcWritableBridge.readDouble(8, __dbResults);
    this.end_lng = JdbcWritableBridge.readDouble(9, __dbResults);
    this.distance = JdbcWritableBridge.readDouble(10, __dbResults);
    this.description = JdbcWritableBridge.readString(11, __dbResults);
    this.street = JdbcWritableBridge.readString(12, __dbResults);
    this.city = JdbcWritableBridge.readString(13, __dbResults);
    this.county = JdbcWritableBridge.readString(14, __dbResults);
    this.state = JdbcWritableBridge.readString(15, __dbResults);
    this.zipcode = JdbcWritableBridge.readString(16, __dbResults);
    this.country = JdbcWritableBridge.readString(17, __dbResults);
    this.timezone = JdbcWritableBridge.readString(18, __dbResults);
    this.airport_code = JdbcWritableBridge.readString(19, __dbResults);
    this.weather_timestamp = JdbcWritableBridge.readString(20, __dbResults);
    this.temperature = JdbcWritableBridge.readDouble(21, __dbResults);
    this.wind_chill = JdbcWritableBridge.readDouble(22, __dbResults);
    this.humidity = JdbcWritableBridge.readDouble(23, __dbResults);
    this.pressure = JdbcWritableBridge.readDouble(24, __dbResults);
    this.visibility = JdbcWritableBridge.readDouble(25, __dbResults);
    this.wind_direction = JdbcWritableBridge.readString(26, __dbResults);
    this.wind_speed = JdbcWritableBridge.readDouble(27, __dbResults);
    this.precipitation = JdbcWritableBridge.readDouble(28, __dbResults);
    this.weather_condition = JdbcWritableBridge.readString(29, __dbResults);
    this.amenity = JdbcWritableBridge.readBoolean(30, __dbResults);
    this.bump = JdbcWritableBridge.readBoolean(31, __dbResults);
    this.crossing = JdbcWritableBridge.readBoolean(32, __dbResults);
    this.give_way = JdbcWritableBridge.readBoolean(33, __dbResults);
    this.junction = JdbcWritableBridge.readBoolean(34, __dbResults);
    this.no_exit = JdbcWritableBridge.readBoolean(35, __dbResults);
    this.railway = JdbcWritableBridge.readBoolean(36, __dbResults);
    this.roundabout = JdbcWritableBridge.readBoolean(37, __dbResults);
    this.station = JdbcWritableBridge.readBoolean(38, __dbResults);
    this.stop = JdbcWritableBridge.readBoolean(39, __dbResults);
    this.traffic_calming = JdbcWritableBridge.readBoolean(40, __dbResults);
    this.traffic_signal = JdbcWritableBridge.readBoolean(41, __dbResults);
    this.turning_loop = JdbcWritableBridge.readBoolean(42, __dbResults);
    this.sunrise_sunset = JdbcWritableBridge.readString(43, __dbResults);
    this.civil_twilight = JdbcWritableBridge.readString(44, __dbResults);
    this.nautical_twilight = JdbcWritableBridge.readString(45, __dbResults);
    this.astronomical_twilight = JdbcWritableBridge.readString(46, __dbResults);
  }
  public void loadLargeObjects(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  public void loadLargeObjects0(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  public void write(PreparedStatement __dbStmt) throws SQLException {
    write(__dbStmt, 0);
  }

  public int write(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeString(id, 1 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(source, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeInteger(severity, 3 + __off, 5, __dbStmt);
    JdbcWritableBridge.writeString(start_time, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(end_time, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeDouble(start_lat, 6 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeDouble(start_lng, 7 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeDouble(end_lat, 8 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeDouble(end_lng, 9 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeDouble(distance, 10 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeString(description, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(street, 12 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(city, 13 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(county, 14 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(state, 15 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(zipcode, 16 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(country, 17 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(timezone, 18 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(airport_code, 19 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(weather_timestamp, 20 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeDouble(temperature, 21 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeDouble(wind_chill, 22 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeDouble(humidity, 23 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeDouble(pressure, 24 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeDouble(visibility, 25 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeString(wind_direction, 26 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeDouble(wind_speed, 27 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeDouble(precipitation, 28 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeString(weather_condition, 29 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBoolean(amenity, 30 + __off, -7, __dbStmt);
    JdbcWritableBridge.writeBoolean(bump, 31 + __off, -7, __dbStmt);
    JdbcWritableBridge.writeBoolean(crossing, 32 + __off, -7, __dbStmt);
    JdbcWritableBridge.writeBoolean(give_way, 33 + __off, -7, __dbStmt);
    JdbcWritableBridge.writeBoolean(junction, 34 + __off, -7, __dbStmt);
    JdbcWritableBridge.writeBoolean(no_exit, 35 + __off, -7, __dbStmt);
    JdbcWritableBridge.writeBoolean(railway, 36 + __off, -7, __dbStmt);
    JdbcWritableBridge.writeBoolean(roundabout, 37 + __off, -7, __dbStmt);
    JdbcWritableBridge.writeBoolean(station, 38 + __off, -7, __dbStmt);
    JdbcWritableBridge.writeBoolean(stop, 39 + __off, -7, __dbStmt);
    JdbcWritableBridge.writeBoolean(traffic_calming, 40 + __off, -7, __dbStmt);
    JdbcWritableBridge.writeBoolean(traffic_signal, 41 + __off, -7, __dbStmt);
    JdbcWritableBridge.writeBoolean(turning_loop, 42 + __off, -7, __dbStmt);
    JdbcWritableBridge.writeString(sunrise_sunset, 43 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(civil_twilight, 44 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(nautical_twilight, 45 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(astronomical_twilight, 46 + __off, 12, __dbStmt);
    return 46;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeString(id, 1 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(source, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeInteger(severity, 3 + __off, 5, __dbStmt);
    JdbcWritableBridge.writeString(start_time, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(end_time, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeDouble(start_lat, 6 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeDouble(start_lng, 7 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeDouble(end_lat, 8 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeDouble(end_lng, 9 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeDouble(distance, 10 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeString(description, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(street, 12 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(city, 13 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(county, 14 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(state, 15 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(zipcode, 16 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(country, 17 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(timezone, 18 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(airport_code, 19 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(weather_timestamp, 20 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeDouble(temperature, 21 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeDouble(wind_chill, 22 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeDouble(humidity, 23 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeDouble(pressure, 24 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeDouble(visibility, 25 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeString(wind_direction, 26 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeDouble(wind_speed, 27 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeDouble(precipitation, 28 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeString(weather_condition, 29 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBoolean(amenity, 30 + __off, -7, __dbStmt);
    JdbcWritableBridge.writeBoolean(bump, 31 + __off, -7, __dbStmt);
    JdbcWritableBridge.writeBoolean(crossing, 32 + __off, -7, __dbStmt);
    JdbcWritableBridge.writeBoolean(give_way, 33 + __off, -7, __dbStmt);
    JdbcWritableBridge.writeBoolean(junction, 34 + __off, -7, __dbStmt);
    JdbcWritableBridge.writeBoolean(no_exit, 35 + __off, -7, __dbStmt);
    JdbcWritableBridge.writeBoolean(railway, 36 + __off, -7, __dbStmt);
    JdbcWritableBridge.writeBoolean(roundabout, 37 + __off, -7, __dbStmt);
    JdbcWritableBridge.writeBoolean(station, 38 + __off, -7, __dbStmt);
    JdbcWritableBridge.writeBoolean(stop, 39 + __off, -7, __dbStmt);
    JdbcWritableBridge.writeBoolean(traffic_calming, 40 + __off, -7, __dbStmt);
    JdbcWritableBridge.writeBoolean(traffic_signal, 41 + __off, -7, __dbStmt);
    JdbcWritableBridge.writeBoolean(turning_loop, 42 + __off, -7, __dbStmt);
    JdbcWritableBridge.writeString(sunrise_sunset, 43 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(civil_twilight, 44 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(nautical_twilight, 45 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(astronomical_twilight, 46 + __off, 12, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.id = null;
    } else {
    this.id = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.source = null;
    } else {
    this.source = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.severity = null;
    } else {
    this.severity = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.start_time = null;
    } else {
    this.start_time = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.end_time = null;
    } else {
    this.end_time = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.start_lat = null;
    } else {
    this.start_lat = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.start_lng = null;
    } else {
    this.start_lng = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.end_lat = null;
    } else {
    this.end_lat = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.end_lng = null;
    } else {
    this.end_lng = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.distance = null;
    } else {
    this.distance = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.description = null;
    } else {
    this.description = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.street = null;
    } else {
    this.street = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.city = null;
    } else {
    this.city = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.county = null;
    } else {
    this.county = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.state = null;
    } else {
    this.state = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.zipcode = null;
    } else {
    this.zipcode = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.country = null;
    } else {
    this.country = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.timezone = null;
    } else {
    this.timezone = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.airport_code = null;
    } else {
    this.airport_code = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.weather_timestamp = null;
    } else {
    this.weather_timestamp = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.temperature = null;
    } else {
    this.temperature = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.wind_chill = null;
    } else {
    this.wind_chill = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.humidity = null;
    } else {
    this.humidity = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.pressure = null;
    } else {
    this.pressure = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.visibility = null;
    } else {
    this.visibility = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.wind_direction = null;
    } else {
    this.wind_direction = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.wind_speed = null;
    } else {
    this.wind_speed = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.precipitation = null;
    } else {
    this.precipitation = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.weather_condition = null;
    } else {
    this.weather_condition = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.amenity = null;
    } else {
    this.amenity = Boolean.valueOf(__dataIn.readBoolean());
    }
    if (__dataIn.readBoolean()) { 
        this.bump = null;
    } else {
    this.bump = Boolean.valueOf(__dataIn.readBoolean());
    }
    if (__dataIn.readBoolean()) { 
        this.crossing = null;
    } else {
    this.crossing = Boolean.valueOf(__dataIn.readBoolean());
    }
    if (__dataIn.readBoolean()) { 
        this.give_way = null;
    } else {
    this.give_way = Boolean.valueOf(__dataIn.readBoolean());
    }
    if (__dataIn.readBoolean()) { 
        this.junction = null;
    } else {
    this.junction = Boolean.valueOf(__dataIn.readBoolean());
    }
    if (__dataIn.readBoolean()) { 
        this.no_exit = null;
    } else {
    this.no_exit = Boolean.valueOf(__dataIn.readBoolean());
    }
    if (__dataIn.readBoolean()) { 
        this.railway = null;
    } else {
    this.railway = Boolean.valueOf(__dataIn.readBoolean());
    }
    if (__dataIn.readBoolean()) { 
        this.roundabout = null;
    } else {
    this.roundabout = Boolean.valueOf(__dataIn.readBoolean());
    }
    if (__dataIn.readBoolean()) { 
        this.station = null;
    } else {
    this.station = Boolean.valueOf(__dataIn.readBoolean());
    }
    if (__dataIn.readBoolean()) { 
        this.stop = null;
    } else {
    this.stop = Boolean.valueOf(__dataIn.readBoolean());
    }
    if (__dataIn.readBoolean()) { 
        this.traffic_calming = null;
    } else {
    this.traffic_calming = Boolean.valueOf(__dataIn.readBoolean());
    }
    if (__dataIn.readBoolean()) { 
        this.traffic_signal = null;
    } else {
    this.traffic_signal = Boolean.valueOf(__dataIn.readBoolean());
    }
    if (__dataIn.readBoolean()) { 
        this.turning_loop = null;
    } else {
    this.turning_loop = Boolean.valueOf(__dataIn.readBoolean());
    }
    if (__dataIn.readBoolean()) { 
        this.sunrise_sunset = null;
    } else {
    this.sunrise_sunset = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.civil_twilight = null;
    } else {
    this.civil_twilight = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.nautical_twilight = null;
    } else {
    this.nautical_twilight = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.astronomical_twilight = null;
    } else {
    this.astronomical_twilight = Text.readString(__dataIn);
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, id);
    }
    if (null == this.source) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, source);
    }
    if (null == this.severity) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.severity);
    }
    if (null == this.start_time) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, start_time);
    }
    if (null == this.end_time) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, end_time);
    }
    if (null == this.start_lat) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.start_lat);
    }
    if (null == this.start_lng) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.start_lng);
    }
    if (null == this.end_lat) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.end_lat);
    }
    if (null == this.end_lng) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.end_lng);
    }
    if (null == this.distance) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.distance);
    }
    if (null == this.description) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, description);
    }
    if (null == this.street) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, street);
    }
    if (null == this.city) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, city);
    }
    if (null == this.county) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, county);
    }
    if (null == this.state) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, state);
    }
    if (null == this.zipcode) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, zipcode);
    }
    if (null == this.country) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, country);
    }
    if (null == this.timezone) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, timezone);
    }
    if (null == this.airport_code) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, airport_code);
    }
    if (null == this.weather_timestamp) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, weather_timestamp);
    }
    if (null == this.temperature) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.temperature);
    }
    if (null == this.wind_chill) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.wind_chill);
    }
    if (null == this.humidity) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.humidity);
    }
    if (null == this.pressure) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.pressure);
    }
    if (null == this.visibility) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.visibility);
    }
    if (null == this.wind_direction) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, wind_direction);
    }
    if (null == this.wind_speed) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.wind_speed);
    }
    if (null == this.precipitation) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.precipitation);
    }
    if (null == this.weather_condition) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, weather_condition);
    }
    if (null == this.amenity) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeBoolean(this.amenity);
    }
    if (null == this.bump) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeBoolean(this.bump);
    }
    if (null == this.crossing) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeBoolean(this.crossing);
    }
    if (null == this.give_way) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeBoolean(this.give_way);
    }
    if (null == this.junction) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeBoolean(this.junction);
    }
    if (null == this.no_exit) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeBoolean(this.no_exit);
    }
    if (null == this.railway) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeBoolean(this.railway);
    }
    if (null == this.roundabout) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeBoolean(this.roundabout);
    }
    if (null == this.station) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeBoolean(this.station);
    }
    if (null == this.stop) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeBoolean(this.stop);
    }
    if (null == this.traffic_calming) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeBoolean(this.traffic_calming);
    }
    if (null == this.traffic_signal) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeBoolean(this.traffic_signal);
    }
    if (null == this.turning_loop) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeBoolean(this.turning_loop);
    }
    if (null == this.sunrise_sunset) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, sunrise_sunset);
    }
    if (null == this.civil_twilight) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, civil_twilight);
    }
    if (null == this.nautical_twilight) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, nautical_twilight);
    }
    if (null == this.astronomical_twilight) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, astronomical_twilight);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, id);
    }
    if (null == this.source) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, source);
    }
    if (null == this.severity) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.severity);
    }
    if (null == this.start_time) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, start_time);
    }
    if (null == this.end_time) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, end_time);
    }
    if (null == this.start_lat) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.start_lat);
    }
    if (null == this.start_lng) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.start_lng);
    }
    if (null == this.end_lat) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.end_lat);
    }
    if (null == this.end_lng) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.end_lng);
    }
    if (null == this.distance) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.distance);
    }
    if (null == this.description) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, description);
    }
    if (null == this.street) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, street);
    }
    if (null == this.city) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, city);
    }
    if (null == this.county) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, county);
    }
    if (null == this.state) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, state);
    }
    if (null == this.zipcode) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, zipcode);
    }
    if (null == this.country) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, country);
    }
    if (null == this.timezone) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, timezone);
    }
    if (null == this.airport_code) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, airport_code);
    }
    if (null == this.weather_timestamp) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, weather_timestamp);
    }
    if (null == this.temperature) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.temperature);
    }
    if (null == this.wind_chill) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.wind_chill);
    }
    if (null == this.humidity) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.humidity);
    }
    if (null == this.pressure) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.pressure);
    }
    if (null == this.visibility) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.visibility);
    }
    if (null == this.wind_direction) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, wind_direction);
    }
    if (null == this.wind_speed) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.wind_speed);
    }
    if (null == this.precipitation) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.precipitation);
    }
    if (null == this.weather_condition) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, weather_condition);
    }
    if (null == this.amenity) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeBoolean(this.amenity);
    }
    if (null == this.bump) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeBoolean(this.bump);
    }
    if (null == this.crossing) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeBoolean(this.crossing);
    }
    if (null == this.give_way) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeBoolean(this.give_way);
    }
    if (null == this.junction) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeBoolean(this.junction);
    }
    if (null == this.no_exit) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeBoolean(this.no_exit);
    }
    if (null == this.railway) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeBoolean(this.railway);
    }
    if (null == this.roundabout) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeBoolean(this.roundabout);
    }
    if (null == this.station) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeBoolean(this.station);
    }
    if (null == this.stop) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeBoolean(this.stop);
    }
    if (null == this.traffic_calming) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeBoolean(this.traffic_calming);
    }
    if (null == this.traffic_signal) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeBoolean(this.traffic_signal);
    }
    if (null == this.turning_loop) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeBoolean(this.turning_loop);
    }
    if (null == this.sunrise_sunset) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, sunrise_sunset);
    }
    if (null == this.civil_twilight) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, civil_twilight);
    }
    if (null == this.nautical_twilight) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, nautical_twilight);
    }
    if (null == this.astronomical_twilight) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, astronomical_twilight);
    }
  }
  private static final DelimiterSet __outputDelimiters = new DelimiterSet((char) 44, (char) 10, (char) 0, (char) 0, false);
  public String toString() {
    return toString(__outputDelimiters, true);
  }
  public String toString(DelimiterSet delimiters) {
    return toString(delimiters, true);
  }
  public String toString(boolean useRecordDelim) {
    return toString(__outputDelimiters, useRecordDelim);
  }
  public String toString(DelimiterSet delimiters, boolean useRecordDelim) {
    StringBuilder __sb = new StringBuilder();
    char fieldDelim = delimiters.getFieldsTerminatedBy();
    __sb.append(FieldFormatter.escapeAndEnclose(id==null?"null":id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(source==null?"null":source, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(severity==null?"null":"" + severity, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(start_time==null?"null":start_time, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(end_time==null?"null":end_time, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(start_lat==null?"null":"" + start_lat, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(start_lng==null?"null":"" + start_lng, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(end_lat==null?"null":"" + end_lat, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(end_lng==null?"null":"" + end_lng, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(distance==null?"null":"" + distance, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(description==null?"null":description, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(street==null?"null":street, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(city==null?"null":city, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(county==null?"null":county, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(state==null?"null":state, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(zipcode==null?"null":zipcode, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(country==null?"null":country, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(timezone==null?"null":timezone, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(airport_code==null?"null":airport_code, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(weather_timestamp==null?"null":weather_timestamp, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(temperature==null?"null":"" + temperature, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(wind_chill==null?"null":"" + wind_chill, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(humidity==null?"null":"" + humidity, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(pressure==null?"null":"" + pressure, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(visibility==null?"null":"" + visibility, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(wind_direction==null?"null":wind_direction, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(wind_speed==null?"null":"" + wind_speed, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(precipitation==null?"null":"" + precipitation, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(weather_condition==null?"null":weather_condition, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(amenity==null?"null":"" + amenity, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(bump==null?"null":"" + bump, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(crossing==null?"null":"" + crossing, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(give_way==null?"null":"" + give_way, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(junction==null?"null":"" + junction, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(no_exit==null?"null":"" + no_exit, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(railway==null?"null":"" + railway, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(roundabout==null?"null":"" + roundabout, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(station==null?"null":"" + station, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(stop==null?"null":"" + stop, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(traffic_calming==null?"null":"" + traffic_calming, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(traffic_signal==null?"null":"" + traffic_signal, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(turning_loop==null?"null":"" + turning_loop, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(sunrise_sunset==null?"null":sunrise_sunset, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(civil_twilight==null?"null":civil_twilight, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(nautical_twilight==null?"null":nautical_twilight, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(astronomical_twilight==null?"null":astronomical_twilight, delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(id==null?"null":id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(source==null?"null":source, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(severity==null?"null":"" + severity, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(start_time==null?"null":start_time, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(end_time==null?"null":end_time, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(start_lat==null?"null":"" + start_lat, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(start_lng==null?"null":"" + start_lng, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(end_lat==null?"null":"" + end_lat, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(end_lng==null?"null":"" + end_lng, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(distance==null?"null":"" + distance, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(description==null?"null":description, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(street==null?"null":street, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(city==null?"null":city, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(county==null?"null":county, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(state==null?"null":state, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(zipcode==null?"null":zipcode, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(country==null?"null":country, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(timezone==null?"null":timezone, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(airport_code==null?"null":airport_code, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(weather_timestamp==null?"null":weather_timestamp, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(temperature==null?"null":"" + temperature, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(wind_chill==null?"null":"" + wind_chill, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(humidity==null?"null":"" + humidity, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(pressure==null?"null":"" + pressure, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(visibility==null?"null":"" + visibility, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(wind_direction==null?"null":wind_direction, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(wind_speed==null?"null":"" + wind_speed, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(precipitation==null?"null":"" + precipitation, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(weather_condition==null?"null":weather_condition, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(amenity==null?"null":"" + amenity, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(bump==null?"null":"" + bump, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(crossing==null?"null":"" + crossing, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(give_way==null?"null":"" + give_way, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(junction==null?"null":"" + junction, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(no_exit==null?"null":"" + no_exit, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(railway==null?"null":"" + railway, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(roundabout==null?"null":"" + roundabout, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(station==null?"null":"" + station, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(stop==null?"null":"" + stop, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(traffic_calming==null?"null":"" + traffic_calming, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(traffic_signal==null?"null":"" + traffic_signal, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(turning_loop==null?"null":"" + turning_loop, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(sunrise_sunset==null?"null":sunrise_sunset, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(civil_twilight==null?"null":civil_twilight, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(nautical_twilight==null?"null":nautical_twilight, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(astronomical_twilight==null?"null":astronomical_twilight, delimiters));
  }
  private static final DelimiterSet __inputDelimiters = new DelimiterSet((char) 44, (char) 10, (char) 0, (char) 0, false);
  private RecordParser __parser;
  public void parse(Text __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(CharSequence __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(byte [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(char [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(ByteBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(CharBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  private void __loadFromFields(List<String> fields) {
    Iterator<String> __it = fields.listIterator();
    String __cur_str = null;
    try {
    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.id = null; } else {
      this.id = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.source = null; } else {
      this.source = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.severity = null; } else {
      this.severity = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.start_time = null; } else {
      this.start_time = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.end_time = null; } else {
      this.end_time = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.start_lat = null; } else {
      this.start_lat = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.start_lng = null; } else {
      this.start_lng = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.end_lat = null; } else {
      this.end_lat = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.end_lng = null; } else {
      this.end_lng = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.distance = null; } else {
      this.distance = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.description = null; } else {
      this.description = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.street = null; } else {
      this.street = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.city = null; } else {
      this.city = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.county = null; } else {
      this.county = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.state = null; } else {
      this.state = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.zipcode = null; } else {
      this.zipcode = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.country = null; } else {
      this.country = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.timezone = null; } else {
      this.timezone = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.airport_code = null; } else {
      this.airport_code = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.weather_timestamp = null; } else {
      this.weather_timestamp = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.temperature = null; } else {
      this.temperature = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.wind_chill = null; } else {
      this.wind_chill = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.humidity = null; } else {
      this.humidity = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.pressure = null; } else {
      this.pressure = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.visibility = null; } else {
      this.visibility = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.wind_direction = null; } else {
      this.wind_direction = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.wind_speed = null; } else {
      this.wind_speed = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.precipitation = null; } else {
      this.precipitation = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.weather_condition = null; } else {
      this.weather_condition = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.amenity = null; } else {
      this.amenity = BooleanParser.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.bump = null; } else {
      this.bump = BooleanParser.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.crossing = null; } else {
      this.crossing = BooleanParser.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.give_way = null; } else {
      this.give_way = BooleanParser.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.junction = null; } else {
      this.junction = BooleanParser.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.no_exit = null; } else {
      this.no_exit = BooleanParser.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.railway = null; } else {
      this.railway = BooleanParser.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.roundabout = null; } else {
      this.roundabout = BooleanParser.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.station = null; } else {
      this.station = BooleanParser.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.stop = null; } else {
      this.stop = BooleanParser.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.traffic_calming = null; } else {
      this.traffic_calming = BooleanParser.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.traffic_signal = null; } else {
      this.traffic_signal = BooleanParser.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.turning_loop = null; } else {
      this.turning_loop = BooleanParser.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.sunrise_sunset = null; } else {
      this.sunrise_sunset = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.civil_twilight = null; } else {
      this.civil_twilight = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.nautical_twilight = null; } else {
      this.nautical_twilight = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.astronomical_twilight = null; } else {
      this.astronomical_twilight = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  private void __loadFromFields0(Iterator<String> __it) {
    String __cur_str = null;
    try {
    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.id = null; } else {
      this.id = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.source = null; } else {
      this.source = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.severity = null; } else {
      this.severity = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.start_time = null; } else {
      this.start_time = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.end_time = null; } else {
      this.end_time = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.start_lat = null; } else {
      this.start_lat = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.start_lng = null; } else {
      this.start_lng = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.end_lat = null; } else {
      this.end_lat = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.end_lng = null; } else {
      this.end_lng = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.distance = null; } else {
      this.distance = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.description = null; } else {
      this.description = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.street = null; } else {
      this.street = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.city = null; } else {
      this.city = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.county = null; } else {
      this.county = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.state = null; } else {
      this.state = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.zipcode = null; } else {
      this.zipcode = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.country = null; } else {
      this.country = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.timezone = null; } else {
      this.timezone = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.airport_code = null; } else {
      this.airport_code = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.weather_timestamp = null; } else {
      this.weather_timestamp = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.temperature = null; } else {
      this.temperature = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.wind_chill = null; } else {
      this.wind_chill = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.humidity = null; } else {
      this.humidity = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.pressure = null; } else {
      this.pressure = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.visibility = null; } else {
      this.visibility = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.wind_direction = null; } else {
      this.wind_direction = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.wind_speed = null; } else {
      this.wind_speed = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.precipitation = null; } else {
      this.precipitation = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.weather_condition = null; } else {
      this.weather_condition = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.amenity = null; } else {
      this.amenity = BooleanParser.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.bump = null; } else {
      this.bump = BooleanParser.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.crossing = null; } else {
      this.crossing = BooleanParser.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.give_way = null; } else {
      this.give_way = BooleanParser.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.junction = null; } else {
      this.junction = BooleanParser.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.no_exit = null; } else {
      this.no_exit = BooleanParser.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.railway = null; } else {
      this.railway = BooleanParser.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.roundabout = null; } else {
      this.roundabout = BooleanParser.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.station = null; } else {
      this.station = BooleanParser.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.stop = null; } else {
      this.stop = BooleanParser.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.traffic_calming = null; } else {
      this.traffic_calming = BooleanParser.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.traffic_signal = null; } else {
      this.traffic_signal = BooleanParser.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.turning_loop = null; } else {
      this.turning_loop = BooleanParser.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.sunrise_sunset = null; } else {
      this.sunrise_sunset = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.civil_twilight = null; } else {
      this.civil_twilight = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.nautical_twilight = null; } else {
      this.nautical_twilight = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.astronomical_twilight = null; } else {
      this.astronomical_twilight = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    accidents o = (accidents) super.clone();
    return o;
  }

  public void clone0(accidents o) throws CloneNotSupportedException {
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("id", this.id);
    __sqoop$field_map.put("source", this.source);
    __sqoop$field_map.put("severity", this.severity);
    __sqoop$field_map.put("start_time", this.start_time);
    __sqoop$field_map.put("end_time", this.end_time);
    __sqoop$field_map.put("start_lat", this.start_lat);
    __sqoop$field_map.put("start_lng", this.start_lng);
    __sqoop$field_map.put("end_lat", this.end_lat);
    __sqoop$field_map.put("end_lng", this.end_lng);
    __sqoop$field_map.put("distance", this.distance);
    __sqoop$field_map.put("description", this.description);
    __sqoop$field_map.put("street", this.street);
    __sqoop$field_map.put("city", this.city);
    __sqoop$field_map.put("county", this.county);
    __sqoop$field_map.put("state", this.state);
    __sqoop$field_map.put("zipcode", this.zipcode);
    __sqoop$field_map.put("country", this.country);
    __sqoop$field_map.put("timezone", this.timezone);
    __sqoop$field_map.put("airport_code", this.airport_code);
    __sqoop$field_map.put("weather_timestamp", this.weather_timestamp);
    __sqoop$field_map.put("temperature", this.temperature);
    __sqoop$field_map.put("wind_chill", this.wind_chill);
    __sqoop$field_map.put("humidity", this.humidity);
    __sqoop$field_map.put("pressure", this.pressure);
    __sqoop$field_map.put("visibility", this.visibility);
    __sqoop$field_map.put("wind_direction", this.wind_direction);
    __sqoop$field_map.put("wind_speed", this.wind_speed);
    __sqoop$field_map.put("precipitation", this.precipitation);
    __sqoop$field_map.put("weather_condition", this.weather_condition);
    __sqoop$field_map.put("amenity", this.amenity);
    __sqoop$field_map.put("bump", this.bump);
    __sqoop$field_map.put("crossing", this.crossing);
    __sqoop$field_map.put("give_way", this.give_way);
    __sqoop$field_map.put("junction", this.junction);
    __sqoop$field_map.put("no_exit", this.no_exit);
    __sqoop$field_map.put("railway", this.railway);
    __sqoop$field_map.put("roundabout", this.roundabout);
    __sqoop$field_map.put("station", this.station);
    __sqoop$field_map.put("stop", this.stop);
    __sqoop$field_map.put("traffic_calming", this.traffic_calming);
    __sqoop$field_map.put("traffic_signal", this.traffic_signal);
    __sqoop$field_map.put("turning_loop", this.turning_loop);
    __sqoop$field_map.put("sunrise_sunset", this.sunrise_sunset);
    __sqoop$field_map.put("civil_twilight", this.civil_twilight);
    __sqoop$field_map.put("nautical_twilight", this.nautical_twilight);
    __sqoop$field_map.put("astronomical_twilight", this.astronomical_twilight);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("id", this.id);
    __sqoop$field_map.put("source", this.source);
    __sqoop$field_map.put("severity", this.severity);
    __sqoop$field_map.put("start_time", this.start_time);
    __sqoop$field_map.put("end_time", this.end_time);
    __sqoop$field_map.put("start_lat", this.start_lat);
    __sqoop$field_map.put("start_lng", this.start_lng);
    __sqoop$field_map.put("end_lat", this.end_lat);
    __sqoop$field_map.put("end_lng", this.end_lng);
    __sqoop$field_map.put("distance", this.distance);
    __sqoop$field_map.put("description", this.description);
    __sqoop$field_map.put("street", this.street);
    __sqoop$field_map.put("city", this.city);
    __sqoop$field_map.put("county", this.county);
    __sqoop$field_map.put("state", this.state);
    __sqoop$field_map.put("zipcode", this.zipcode);
    __sqoop$field_map.put("country", this.country);
    __sqoop$field_map.put("timezone", this.timezone);
    __sqoop$field_map.put("airport_code", this.airport_code);
    __sqoop$field_map.put("weather_timestamp", this.weather_timestamp);
    __sqoop$field_map.put("temperature", this.temperature);
    __sqoop$field_map.put("wind_chill", this.wind_chill);
    __sqoop$field_map.put("humidity", this.humidity);
    __sqoop$field_map.put("pressure", this.pressure);
    __sqoop$field_map.put("visibility", this.visibility);
    __sqoop$field_map.put("wind_direction", this.wind_direction);
    __sqoop$field_map.put("wind_speed", this.wind_speed);
    __sqoop$field_map.put("precipitation", this.precipitation);
    __sqoop$field_map.put("weather_condition", this.weather_condition);
    __sqoop$field_map.put("amenity", this.amenity);
    __sqoop$field_map.put("bump", this.bump);
    __sqoop$field_map.put("crossing", this.crossing);
    __sqoop$field_map.put("give_way", this.give_way);
    __sqoop$field_map.put("junction", this.junction);
    __sqoop$field_map.put("no_exit", this.no_exit);
    __sqoop$field_map.put("railway", this.railway);
    __sqoop$field_map.put("roundabout", this.roundabout);
    __sqoop$field_map.put("station", this.station);
    __sqoop$field_map.put("stop", this.stop);
    __sqoop$field_map.put("traffic_calming", this.traffic_calming);
    __sqoop$field_map.put("traffic_signal", this.traffic_signal);
    __sqoop$field_map.put("turning_loop", this.turning_loop);
    __sqoop$field_map.put("sunrise_sunset", this.sunrise_sunset);
    __sqoop$field_map.put("civil_twilight", this.civil_twilight);
    __sqoop$field_map.put("nautical_twilight", this.nautical_twilight);
    __sqoop$field_map.put("astronomical_twilight", this.astronomical_twilight);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
