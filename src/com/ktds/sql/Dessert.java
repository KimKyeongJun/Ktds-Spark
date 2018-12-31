package com.ktds.sql;

import java.io.Serializable;

public class Dessert implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2530640933876345012L;

	private final String menuId;
	private final String name;
	private final int price;
	private final int kcal;

	public Dessert(String[] args) {
		this.menuId = args[0];
		this.name = args[1];
		this.price = Integer.parseInt(args[2]);
		this.kcal = Integer.parseInt(args[3]);
	}

	public String getMenuId() {
		return menuId;
	}

	public String getName() {
		return name;
	}

	public int getPrice() {
		return price;
	}

	public int getKcal() {
		return kcal;
	}

}
