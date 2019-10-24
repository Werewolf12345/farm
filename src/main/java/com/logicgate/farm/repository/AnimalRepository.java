package com.logicgate.farm.repository;

import com.logicgate.farm.domain.Animal;
import com.logicgate.farm.domain.Barn;
import com.logicgate.farm.domain.Color;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface AnimalRepository extends JpaRepository<Animal, Long> {

  // additional methods can be defined here

  List<Animal> findAllByBarn(Barn barn);

  Animal findFirstByBarn(Barn barn);

  List<Animal> findAllByFavoriteColor(Color color);

}
