package wdsr.exercise5.dao;


import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.List;
import java.util.Optional;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;

import wdsr.exercise5.model.Trade;

@Repository
public class TradeDao {

    @Autowired
    JdbcTemplate jdbcTemplate;


    
    public int insertTrade(Trade trade) {
    	
    	KeyHolder keyHolder = new GeneratedKeyHolder();
    	String sqlString = "INSERT INTO Trade (asset, amount, date) VALUES (?, ?, ?)";
    	
        jdbcTemplate.update(new PreparedStatementCreator() {
			
			@Override
			public PreparedStatement createPreparedStatement(Connection con) throws SQLException {
				PreparedStatement ps = con.prepareStatement(sqlString, PreparedStatement.RETURN_GENERATED_KEYS);
				
				
				ps.setString(1, trade.getAsset());
				ps.setDouble(2, trade.getAmount());
				ps.setDate(3, new Date(trade.getDate().getTime()));
				
				return ps;
			}
		}, keyHolder);
        
        return (int) keyHolder.getKey();
    }

    public Optional<Trade> extractTrade(int id) {
    	String sqlString = "SELECT * FROM TRADE WHERE ID="+id;
    	
    	List<Trade> trades  = jdbcTemplate.query(sqlString,new BeanPropertyRowMapper<Trade>(Trade.class));
    	
    	if(trades.isEmpty()){
    		return Optional.empty();
    	}
    	return Optional.of(trades.get(0));
		
	}
    
    public void extractTrade(int id, RowCallbackHandler rch) {
    	
    	String sqlString = "SELECT * FROM TRADE WHERE ID="+id;
    	
    	jdbcTemplate.query(sqlString, rch); 
    }

    
    
    public void updateTrade(int id, Trade trade) {
    	
    	String sqlString = "UPDATE TRADE SET ASSET = ?, AMOUNT = ?, DATE = ? WHERE ID = ?";
        Object[] params = new Object[] { trade.getAsset(), trade.getAmount(), trade.getDate(),id };
        int[] types = { Types.VARCHAR, Types.DOUBLE, Types.DATE,Types.INTEGER  };
        jdbcTemplate.update(sqlString, params, types);

    }

    public void deleteTrade(int id) {
    	String sqlString = "DELETE FROM TRADE WHERE ID = ?";
        jdbcTemplate.update(sqlString, id);
    }

}
