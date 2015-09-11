package cn.edu.pku.zx.ali.predict.config;

import java.util.Date;

public class Configuration {
	@SuppressWarnings({ "unchecked", "deprecation" })
	public static IConfigurable getConfig() 
	{
		String clsName="cn.edu.pku.zx.ali.predict.config.Config_";
		Date today=new Date();
		clsName+=(today.getMonth()+1)+"_"+today.getDate()+"_1st";
		Class<IConfigurable> cls=null;
		try {
			cls = (Class<IConfigurable>) Class.forName(clsName);
			return cls.newInstance();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;		
	}
}
