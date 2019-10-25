package com.logicgate.farm.service;

import com.logicgate.farm.domain.Animal;
import com.logicgate.farm.domain.Barn;
import com.logicgate.farm.domain.Color;
import com.logicgate.farm.repository.AnimalRepository;
import com.logicgate.farm.repository.BarnRepository;
import com.logicgate.farm.util.FarmUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;

@Service
@Transactional
public class AnimalServiceImpl implements AnimalService {

  private final AnimalRepository animalRepository;

  private final BarnRepository barnRepository;

  @Autowired
  public AnimalServiceImpl(AnimalRepository animalRepository, BarnRepository barnRepository) {
    this.animalRepository = animalRepository;
    this.barnRepository = barnRepository;
  }

  @Override
  @Transactional(readOnly = true)
  public List<Animal> findAll() {
    return animalRepository.findAll();
  }

  @Override
  public void deleteAll() {
    animalRepository.deleteAll();
  }

  @Override
  public Animal addToFarm(Animal animal) {

    // TODO: implementation of this method

    List<Barn> barnListOfColor = barnRepository.findAllByColor(animal.getFavoriteColor());

    if (barnListOfColor.isEmpty()) {

      Barn newBarn = new Barn("Barn - " + animal.getFavoriteColor(), animal.getFavoriteColor());

      barnRepository.saveAndFlush(newBarn);
      animal.setBarn(newBarn);
      animalRepository.saveAndFlush(animal);

    } else {

      List<Animal> animalResult = animalRepository.findAllByFavoriteColor(animal.getFavoriteColor());

      Map<Barn, List<Animal>> barnAnimalMap = animalResult.stream()
        .collect(Collectors.groupingBy(Animal::getBarn));

      Optional<Entry<Barn, List<Animal>>> barnWithMaxSpace = barnAnimalMap.entrySet().stream()
        .filter(e -> e.getValue().size() < FarmUtils.barnCapacity())
        .min(Comparator.comparingInt(e -> e.getValue().size()));

      Barn barn = barnWithMaxSpace.isPresent() ? barnWithMaxSpace.get().getKey()
        : new Barn("Barn - " + animal.getFavoriteColor(), animal.getFavoriteColor());

      barnRepository.saveAndFlush(barn);
      animal.setBarn(barn);
      animalRepository.saveAndFlush(animal);

      while (!isBarnBalanced(animal.getFavoriteColor())) {
        balanceBarn(animal.getFavoriteColor());
      }
    }

    return animal;
  }

  @Override
  public void addToFarm(List<Animal> animals) {
    animals.forEach(this::addToFarm);
  }

  @Override
  public void removeFromFarm(Animal animal) {

    // TODO: implementation of this method

    Barn barn = animal.getBarn();
    Color color = animal.getFavoriteColor();

    animalRepository.delete(animal);
    animalRepository.flush();

    if (animalRepository.findAllByBarn(barn).isEmpty()) {
      barnRepository.delete(barn);
      barnRepository.flush();
    }

    while (!isBarnBalanced(color)) {
      balanceBarn(color);
    }
  }

  @Override
  public void removeFromFarm(List<Animal> animals) {
    animals.forEach(animal -> removeFromFarm(animalRepository.getOne(animal.getId())));
  }

  private boolean isBarnBalanced(Color color) {

    final List<Animal> animalList = animalRepository.findAllByFavoriteColor(color);
    final List<Barn> barnList = barnRepository.findAllByColor(color);

    if (animalList.isEmpty() || barnList.isEmpty()) {
      return true;
    }

    int minCapacity = barnList.stream()
      .mapToInt(Barn::getCapacity)
      .min()
      .orElse(FarmUtils.barnCapacity());

    Map<Barn, Long> barnNumAnimalsMap =
      animalList.stream().collect(Collectors.groupingBy(Animal::getBarn, Collectors.counting()));

    List<Integer> unusedCapacity = barnNumAnimalsMap.entrySet().stream()
      .map(e -> (int) (e.getKey().getCapacity() - e.getValue()))
      .collect(Collectors.toList());

    int totalUnusedCapacity = unusedCapacity.stream()
      .mapToInt(i -> i)
      .sum();

    return minCapacity > totalUnusedCapacity
      && (Collections.max(unusedCapacity) - Collections.min(unusedCapacity)) <= 1;
  }

  private void balanceBarn(Color color) {

    final List<Animal> animalList = animalRepository.findAllByFavoriteColor(color);
    final List<Barn> barnList = barnRepository.findAllByColor(color);

    Map<Barn, Long> barnNumAnimalsMap = animalList.stream()
      .collect(Collectors.groupingBy(Animal::getBarn, Collectors.counting()));

    List<Integer> unusedCapacity = barnNumAnimalsMap.entrySet().stream()
      .map(e -> (int) (e.getKey().getCapacity() - e.getValue()))
      .collect(Collectors.toList());

    int totalUnusedCapacity = unusedCapacity.stream()
      .mapToInt(i -> i)
      .sum();

    int minCapacity = barnList.stream()
      .mapToInt(Barn::getCapacity)
      .min()
      .orElse(FarmUtils.barnCapacity());

    if (totalUnusedCapacity >= minCapacity) {
      Barn barnToDemolish = barnList.stream()
        .min(Comparator.comparingInt(Barn::getCapacity))
        .get();

      Queue<Animal> animalsToMove = new LinkedList<>(animalRepository.findAllByBarn(barnToDemolish));
      barnNumAnimalsMap.remove(barnToDemolish);

      barnNumAnimalsMap.forEach((barn, numAnimals) -> {
        for (int i = 0; i < barn.getCapacity() - numAnimals; i++) {
          Animal animal = animalsToMove.poll();
          if(animal != null) {
            animal.setBarn(barn);
          } else {
            break;
          }
        }
      });

      barnRepository.delete(barnToDemolish);
      barnRepository.flush();
      animalRepository.flush();
    }

    Barn barnWithMinNumber = Collections.min(barnNumAnimalsMap.entrySet(), Entry.comparingByValue()).getKey();
    barnNumAnimalsMap.remove(barnWithMinNumber);
    Barn barnWithMaxNumber = Collections.max(barnNumAnimalsMap.entrySet(), Entry.comparingByValue()).getKey();

    Animal animal = animalRepository.findFirstByBarn(barnWithMaxNumber);

    animal.setBarn(barnWithMinNumber);

    animalRepository.flush();
  }
}
